#include "threadpool.h"

const int TASK_MAX_THRESHHOLD = INT32_MAX;
const int THREAD_MAX_THRESHHOLD = 10;
const int THREAD_MAX_IDLE_TIME = 10; //单位： 秒

Threadpool::Threadpool() : 
	initThreadSize_(0),
	threadSizeThreshHold_(THREAD_MAX_THRESHHOLD),
	taskSize_(0),
	idleThreadSize_(0),
	curThreadSize_(0),
	taskQueMaxThreadHold_(TASK_MAX_THRESHHOLD),
	poolMode_(PoolMode::MODE_FIXED),
	isPoolRunning_(false){


}

Threadpool::~Threadpool() {
	isPoolRunning_ = false;
	std::unique_lock<std::mutex> lock(taskQueMtx_);
	notEmpty_.notify_all();
    exitCond_.wait(lock, [&]()->bool { return threads_.size() == 0; });
}

void Threadpool::setMode(PoolMode mode) {
	if (isPoolRunning_) {
		return;
	}
	poolMode_ = mode;
}

void Threadpool::setTaskQueMaxThreshHold(int threshold) {
	taskQueMaxThreadHold_ = threshold;
}

void Threadpool::setThreadSizeThreshHold(int threshold) {
	if (isPoolRunning_) {
		return;
	}
	if (poolMode_ == PoolMode::MODE_CACHED) {
		threadSizeThreshHold_ = threshold;
	}
}

//给线程池提交任务，用户调用该接口，传入任务对象
Result Threadpool::submitTask(std::shared_ptr<Task> sp) {
	//获取锁
	std::unique_lock<std::mutex> lock(taskQueMtx_);
	//线程的通信 等待任务队列有空余,且最长阻塞不能超过1s
	if (!notFull_.wait_for(lock, std::chrono::seconds(1), [&]()->bool {
		return taskQue_.size() < (size_t)taskQueMaxThreadHold_;
		})) {
		std::cerr << "task queue is full,submit task fail," << std::endl;
		return Result(sp,false);
	}
	//如果有空余 则任务放入任务队列中
	taskQue_.emplace(sp);
	taskSize_++;
	//此时任务队列不为空，在notEmpty_上通知
	notEmpty_.notify_all();

	//cached模式，任务处理比较紧急，场景:小而快的任务
	//需要根据任务数量和空闲线程数量，判断是否需要创建新的线程出来
	if (poolMode_ == PoolMode::MODE_CACHED &&
		taskSize_ > idleThreadSize_&&
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


	return Result(sp);
}

void Threadpool::start(int initThreadSize) {
	//设置线程的运行状态
	isPoolRunning_ = true;

	//记录初始线程个数
	initThreadSize_ = initThreadSize;
	curThreadSize_ = initThreadSize;
	//创建线程对象
	for (int i = 0; i < initThreadSize_; ++i) {
		auto ptr = std::make_unique<Thread>(std::bind(&Threadpool::threadFunc, this,std::placeholders::_1));
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

//线程函数 线程池的所有线程从任务队列里消费任务
void Threadpool::threadFunc(int threadId) {  //线程函数返回，相应线程也就结束了
	/*std::cout << "begin threadFunc tid : " << std::this_thread::get_id() << std::endl;
	std::cout << "end threadFunc" << std::this_thread::get_id() << std::endl;*/
	auto lastTime = std::chrono::high_resolution_clock().now();

	//所有任务必须执行完成，线程池才可以回收所有线程资源
	for (;;) {
		std::shared_ptr<Task> task;
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
					}else{
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
			task->exec();
		}

		idleThreadSize_++; //任务执行完成，空闲线程数量++;
		lastTime = std::chrono::high_resolution_clock().now();  //更新线程执行完任务的时间
	}
}

bool Threadpool::checkRunningState() const {
	return isPoolRunning_;
}
/* Task成员函数实现 */

Task::Task() :result_(nullptr){

}

void Task::exec() {
	if (result_ != nullptr) {
		result_->setVal(run());
	}
}

void Task::setResult(Result* res) {
	result_ = res;
}

/* 线程成员函数实现 */
int Thread::generateId_ = 0;

Thread::Thread(ThreadFunc func) : func_(func),ThreadId_(generateId_++){}

Thread::~Thread() {}

void Thread::start() {
	//创建一个线程来执行一个线程函数
	std::thread t(func_,ThreadId_);
	t.detach();
}

int Thread::getId() const {
	return ThreadId_;
}

/* Result成员函数实现 */

Result::Result(std::shared_ptr<Task> task, bool isVaild)
	: task_(task),isVaild_(isVaild){
	task_->setResult(this);
}

Any Result::get(){   //用户调用
	if (!isVaild_) {
		return "";
	}
	sem_.wait();     //当用户发起请求get时，若线程函数未执行完成，则会阻塞。
	return std::move(any_);
}

void Result::setVal(Any any) {   //线程池的线程发起调用
	any_ = std::move(any);
	sem_.post();          //拿到返回值后，信号量资源++；
}
