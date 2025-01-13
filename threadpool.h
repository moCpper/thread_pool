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

//Any类型：可以接收任意数据的类型
class Any {
public:
	template<typename T>
	Any(T data):base_(std::make_unique<Derive<T>>(data)){}

	Any() = default;
	Any(const Any&) = delete;
	Any& operator=(const Any&) = delete;
	Any(Any&&) = default;
	Any& operator=(Any&&) = default;
	~Any() = default;

	template<typename T>
	T cast_() {
		Derive<T>* p = dynamic_cast<Derive<T>*>(base_.get());
		if (p == nullptr) {
			throw "type is unmatch!";
		}
		return p->data_;
	}
private:
	class Base {
	public:
		virtual ~Base() = default;
	};
	template<typename T>
	class Derive : public Base {
	public:
		Derive(T data):data_(data){}
		T data_;
	};

	std::unique_ptr<Base> base_;
};

//实现一个信号量类
class Semaphore {
public:
	Semaphore(int limit = 0): resLimit_(limit){}
	~Semaphore() = default;

	//获取一个信号量资源
	void wait(){
		std::unique_lock<std::mutex> lock(mtx_);
		cond_.wait(lock, [&]()->bool {return resLimit_ > 0; });
		resLimit_--;
	}

	//增加一个信号量资源
	void post() {
		std::unique_lock<std::mutex> lock(mtx_);
		resLimit_++;
		cond_.notify_all();
	}
private:
	int resLimit_;
	std::mutex mtx_;
	std::condition_variable cond_;
};

class Task;
//实现接收提交到线程池的task任务执行完成后的返回值类型Result
class Result {
public:
	Result(std::shared_ptr<Task> task, bool isVaild = true);
	~Result() = default;
	Any get();    
	void setVal(Any any);
private:
	Any any_;
	Semaphore sem_;
	std::shared_ptr<Task> task_;  
	std::atomic_bool isVaild_; //返回值是否有效
};

enum class PoolMode {
	MODE_FIXED,   //固定数量线程
	MODE_CACHED,   //可动态增加的线程
};

//任务纯虚基类
class Task {
public:
	Task();
	~Task() = default;
	void exec();
	void setResult(Result* res);
private:
	//用户可以自定义任意任务类型，从Task继承，override run纯虚函数,实现自定义处理
	virtual Any run() = 0;

	Result* result_;
};

//线程类型
class Thread {
public:
	//线程函数对象类型
	using ThreadFunc = std::function<void(int)>;
	Thread(ThreadFunc func);
	~Thread();
	//启动线程
	void start();
	//获取线程ID
	int getId() const;
private:
	ThreadFunc func_;
	static int generateId_;
	int ThreadId_;
};
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
class Threadpool{
public:
	Threadpool();
	~Threadpool();

	Threadpool(const Threadpool&) = delete;
	Threadpool& operator=(const Threadpool&) = delete;

	//设置线程池工作模式
	void setMode(PoolMode mode);

	//设置task任务队列上限阈值
	void setTaskQueMaxThreshHold(int threshold);

	//设置线程池cached模式下线程阈值
	void setThreadSizeThreshHold(int threshold);

	//给线程池提交任务
	Result submitTask(std::shared_ptr<Task> sp);

	//开启线程池
	void start(int initThreadSize = 4);
private:
	//定义线程函数
	void threadFunc(int threadId);

	//检查pool的运行状态
	bool checkRunningState() const;

	//std::vector<std::unique_ptr<Thread>> threads_; //线程列表
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;
	int initThreadSize_;  //初始的线程数量
	int threadSizeThreshHold_;//线程数量的上限阈值
	std::atomic_int curThreadSize_;//记录当前线程池里总线程的数量
	std::atomic_int idleThreadSize_;//记录空闲线程的数量

	std::queue<std::shared_ptr<Task>> taskQue_;  //任务队列
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