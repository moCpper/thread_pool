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

//Any���ͣ����Խ����������ݵ�����
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

//ʵ��һ���ź�����
class Semaphore {
public:
	Semaphore(int limit = 0): resLimit_(limit){}
	~Semaphore() = default;

	//��ȡһ���ź�����Դ
	void wait(){
		std::unique_lock<std::mutex> lock(mtx_);
		cond_.wait(lock, [&]()->bool {return resLimit_ > 0; });
		resLimit_--;
	}

	//����һ���ź�����Դ
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
//ʵ�ֽ����ύ���̳߳ص�task����ִ����ɺ�ķ���ֵ����Result
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
	std::atomic_bool isVaild_; //����ֵ�Ƿ���Ч
};

enum class PoolMode {
	MODE_FIXED,   //�̶������߳�
	MODE_CACHED,   //�ɶ�̬���ӵ��߳�
};

//���������
class Task {
public:
	Task();
	~Task() = default;
	void exec();
	void setResult(Result* res);
private:
	//�û������Զ��������������ͣ���Task�̳У�override run���麯��,ʵ���Զ��崦��
	virtual Any run() = 0;

	Result* result_;
};

//�߳�����
class Thread {
public:
	//�̺߳�����������
	using ThreadFunc = std::function<void(int)>;
	Thread(ThreadFunc func);
	~Thread();
	//�����߳�
	void start();
	//��ȡ�߳�ID
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
			// �̴߳���...
		}
}
pool.submitTask(std::make_shared<MyTask>());
*/
//�̳߳�����
class Threadpool{
public:
	Threadpool();
	~Threadpool();

	Threadpool(const Threadpool&) = delete;
	Threadpool& operator=(const Threadpool&) = delete;

	//�����̳߳ع���ģʽ
	void setMode(PoolMode mode);

	//����task�������������ֵ
	void setTaskQueMaxThreshHold(int threshold);

	//�����̳߳�cachedģʽ���߳���ֵ
	void setThreadSizeThreshHold(int threshold);

	//���̳߳��ύ����
	Result submitTask(std::shared_ptr<Task> sp);

	//�����̳߳�
	void start(int initThreadSize = 4);
private:
	//�����̺߳���
	void threadFunc(int threadId);

	//���pool������״̬
	bool checkRunningState() const;

	//std::vector<std::unique_ptr<Thread>> threads_; //�߳��б�
	std::unordered_map<int, std::unique_ptr<Thread>> threads_;
	int initThreadSize_;  //��ʼ���߳�����
	int threadSizeThreshHold_;//�߳�������������ֵ
	std::atomic_int curThreadSize_;//��¼��ǰ�̳߳������̵߳�����
	std::atomic_int idleThreadSize_;//��¼�����̵߳�����

	std::queue<std::shared_ptr<Task>> taskQue_;  //�������
	std::atomic_int taskSize_;//���������
	int taskQueMaxThreadHold_;  //�����������������ֵ

	std::mutex taskQueMtx_;  //��֤������е��̰߳�ȫ
	std::condition_variable notFull_;//��ʾ������в���
	std::condition_variable notEmpty_; //��ʾ������в���
	std::condition_variable exitCond_; // �ȴ��߳���Դȫ������

	//��ǰ�̳߳صĹ���ģʽ
	PoolMode poolMode_;

	std::atomic_bool isPoolRunning_;//��ʾ��ǰ�̵߳�����״̬
};

#endif