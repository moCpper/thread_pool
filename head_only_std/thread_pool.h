#ifndef THREADPOOL_H
#define THREADPOOL_H

#include<vector>
#include<queue>
#include<memory>
#include<atomic>
#include<mutex>
#include<condition_variable>
#include<functional>
#include<thread>
#include<chrono>
#include<iostream>
#include<unordered_map>
#include<future>
#include<thread>

const int TASK_MAX_THRESHHOLD = INT32_MAX;
const int THREAD_MAX_THRESHHOLD = 10;
const int THREAD_MAX_IDLE_TIME = 10; //单位： 秒

enum class PoolMode {
	MODE_FIXED,   //固定数量线程
	MODE_CACHED,   //可动态增加的线程
};


//线程类型
class Thread {
public:
	//线程函数对象类型
	using ThreadFunc = std::function<void(int)>;
	Thread(ThreadFunc func) : func_(func), 
		ThreadId_(generateId_++) {}
	~Thread() = default;
	//启动线程
	void start() {
		//创建一个线程来执行一个线程函数
		std::thread t(func_, ThreadId_);
		t.detach();
	}
	//获取线程ID
	int getId() const {
		return ThreadId_;
	}
private:
	ThreadFunc func_;
	static int generateId_;
	int ThreadId_;
};
int Thread::generateId_ = 0;
/*
example:
Threaadpool pool;
pool.start(4);
class MyTask : public Task{
	public:
		virtual void run() override{
			// 线程代码...
		}
}
pool.submitTask(std::make_shared<MyTask>());
*/
//线程池类型
class Threadpool {
public:
	Threadpool() :
		initThreadSize_(0),
		threadSizeThreshHold_(THREAD_MAX_THRESHHOLD),
		taskSize_(0),
		idleThreadSize_(0),
		curThreadSize_(0),
		taskQueMaxThreadHold_(TASK_MAX_THRESHHOLD),
		poolMode_(PoolMode::MODE_FIXED),
		isPoolRunning_(false) {}

	~Threadpool() {
		isPoolRunning_ = false;
		std::unique_lock<std::mutex> lock(taskQueMtx_);
		notEmpty_.notify_all();
		exitCond_.wait(lock, [&]()->bool { return threads_.size() == 0; });
	}

	Threadpool(const Threadpool&) = delete;
	Threadpool& operator=(const Threadpool&) = delete;

	//设置线程池工作模式
	void setMode(PoolMode mode) {
		if (isPoolRunning_) {
			return;
		}
		poolMode_ = mode;
	}

	//设置task任务队列上限阈值
	void setTaskQueMaxThreshHold(int threshold) {
		taskQueMaxThreadHold_ = threshold;
	}

	//设置线程池cached模式下线程阈值
	void setThreadSizeThreshHold(int threshold) {
		if (isPoolRunning_) {
			return;
		}
		if (poolMode_ == PoolMode::MODE_CACHED) {
			threadSizeThreshHold_ = threshold;
		}
	}

	//给线程池提交任务
	template<typename Func,typename... Args>
	auto submitTask(Func&& func,Args&&... args) -> std::future<decltype(func(args...))>{

		using RType = decltype(func(args...));
		auto task = std::make_shared<std::packaged_task<RType()>>(std::bind(std::forward<Func>(func),
			std::forward<Args>(args)...));
		std::future<RType> result = task->get_future();

		//获取锁
		std::unique_lock<std::mutex> lock(taskQueMtx_);
		//线程的通信 等待任务队列有空余,且最长阻塞不能超过1s
		if (!notFull_.wait_for(lock, std::chrono::seconds(1), [&]()->bool {
			return taskQue_.size() < (size_t)taskQueMaxThreadHold_;
			})) {
			std::cerr << "task queue is full,submit task fail," << std::endl;
			
			auto task = std::make_shared<std::packaged_task<RType()>>([]()->RType{
				return RType();
				});
			(*task)();
			return task->get_future();
		}
		//如果有空余 则任务放入任务队列中
		//taskQue_.emplace(sp);
		taskQue_.emplace([task]() {(*task)(); });
		taskSize_++;
		//此时任务队列不为空，在notEmpty_上通知
		notEmpty_.notify_all();

		//cached模式，任务处理比较紧急，场景:小而快的任务
		//需要根据任务数量和空闲线程数量，判断是否需要创建新的线程出来
		if (poolMode_ == PoolMode::MODE_CACHED &&
			taskSize_ > idleThreadSize_ &&
			curThreadSize_ < threadSizeThreshHold_) {
			//创建新的线程对象
			std::cout << " >>> create new thread..." << std::endl;
			auto ptr = std::make_unique<Thread>(std::bind(&Threadpool::threadFunc, this, std::placeholders::_1));
			int threadID = ptr->getId();
			threads_.emplace(threadID, std::move(ptr));
			threads_[threadID]->start();
			curThreadSize_++;
			idleThreadSize_++;
		}

		return result;
	}

	//开启线程池
	void start(int initThreadSize = 4) {
		//设置线程的运行状态
		isPoolRunning_ = true;

		//记录初始线程个数
		initThreadSize_ = initThreadSize;
		curThreadSize_ = initThreadSize;
		//创建线程对象
		for (int i = 0; i < initThreadSize_; ++i) {
			auto ptr = std::make_unique<Thread>(std::bind(&Threadpool::threadFunc, this, std::placeholders::_1));
			int threadID = ptr->getId();
			threads_.emplace(threadID, std::move(ptr));
			//threads_.emplace_back(std::move(ptr));
		}
		//启动所有线程
		for (int i = 0; i < initThreadSize_; ++i) {
			threads_[i]->start();
			idleThreadSize_++;  //记录空闲线程的数量
		}
	}
private:
	//定义线程函数
	void threadFunc(int threadId) {
		auto lastTime = std::chrono::high_resolution_clock().now();

		//所有任务必须执行完成，线程池才可以回收所有线程资源
		for (;;) {
			Task task;
			{
				std::unique_lock<std::mutex> lock(taskQueMtx_);

				std::cout << "tid : " << std::this_thread::get_id() << "尝试获取任务..."
					<< std::endl;

				//cached模式下，有可能已经创建了许多线程，但是空闲时间超过60s,应该把多余的线程
				//结束回收掉(超过initThreadSize_数量的线程要进行回收)
				//当前时间 - 上一次线程执行的时间 > 60s 
				//锁 + 双重判断
				while (taskQue_.size() == 0) {
					//线程池结束，回收线程资源
					if (!isPoolRunning_) {
						threads_.erase(threadId);
						std::cout << "thread id :" << std::this_thread::get_id() << "exit!"
							<< std::endl;
						exitCond_.notify_all();
						return;   //线程函数结束，线程结束
					}

					if (poolMode_ == PoolMode::MODE_CACHED) {
						if (std::cv_status::timeout == notEmpty_.wait_for(lock, std::chrono::seconds(1))) {  //cv_status为wait_for返回值（枚举类）
							auto now = std::chrono::high_resolution_clock().now();
							auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);
							if (dur.count() >= THREAD_MAX_IDLE_TIME &&
								curThreadSize_ > initThreadSize_) {
								//回收该线程
								threads_.erase(threadId);
								curThreadSize_--;
								idleThreadSize_--;

								std::cout << "thread id :" << std::this_thread::get_id() << "exit!" << std::endl;
								return;
							}
						}
					}
					else {
						notEmpty_.wait(lock);
					}

					//if (!isPoolRunning_) {
					//	threads_.erase(threadId);
					//	std::cout << "thread id :" << std::this_thread::get_id()
					//		<< "exit!" << std::endl;
					//	exitCond_.notify_all();
					//	return;
					//}
				}

				idleThreadSize_--;

				std::cout << "tid : " << std::this_thread::get_id() << "获取任务成功..."
					<< std::endl;

				task = taskQue_.front();
				taskQue_.pop();
				taskSize_--;

				//如果依然有其他任务，则通知其他线程执行任务
				if (taskQue_.size() > 0) {
					notEmpty_.notify_all();
				}

				notFull_.notify_all();

			}
			//当前线程执行获取的任务
			if (task != nullptr) {
				task();
			}

			idleThreadSize_++; //任务执行完成，空闲线程数量++;
			lastTime = std::chrono::high_resolution_clock().now();  //更新线程执行完任务的时间
		}
	}

	//检查pool的运行状态
	bool checkRunningState() const {
		return isPoolRunning_;
	}

	//std::vector<std::unique_ptr<Thread>> threads_; //线程列表
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;
	int initThreadSize_;  //初始的线程数量
	int threadSizeThreshHold_;//线程数量的上限阈值
	std::atomic_int curThreadSize_;//记录当前线程池里总线程的数量
	std::atomic_int idleThreadSize_;//记录空闲线程的数量

	using Task = std::function<void()>;
	std::queue<Task> taskQue_;  //任务队列
	std::atomic_int taskSize_;//任务的数量
	int taskQueMaxThreadHold_;  //任务队列数量上限阈值

	std::mutex taskQueMtx_;  //保证任务队列的线程安全
	std::condition_variable notFull_;//表示任务队列不满
	std::condition_variable notEmpty_; //表示任务队列不空
	std::condition_variable exitCond_; // 等待线程资源全部回收

	//当前线程池的工作模式
	PoolMode poolMode_;

	std::atomic_bool isPoolRunning_;//表示当前线程的启动状态
};

#endif