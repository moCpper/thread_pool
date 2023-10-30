#include "threadpool.h"

const int TASK_MAX_THRESHHOLD = 1024;

Threadpool::Threadpool() : 
	initThreadSize_(0),
	taskSize_(0),
	taskQueMaxThreadHold_(TASK_MAX_THRESHHOLD),
	poolMode_(PoolMode::MODE_FIXED){


}

Threadpool::~Threadpool() {}

void Threadpool::setMode(PoolMode mode) {
	poolMode_ = mode;
}

void Threadpool::setTaskQueMaxThreshHold(int threshold) {
	taskQueMaxThreadHold_ = threshold;
}

//���̳߳��ύ�����û����øýӿڣ������������
Result Threadpool::submitTask(std::shared_ptr<Task> sp) {
	//��ȡ��
	std::unique_lock<std::mutex> lock(taskQueMtx_);
	//�̵߳�ͨ�� �ȴ���������п���,����������ܳ���1s
	if (!notFull_.wait_for(lock, std::chrono::seconds(1), [&]()->bool {
		return taskQue_.size() < (size_t)taskQueMaxThreadHold_;
		})) {
		std::cerr << "task queue is full,submit task fail," << std::endl;
		return Result(sp,false);
	}
	//����п��� ������������������
	taskQue_.emplace(sp);
	taskSize_++;
	//��ʱ������в�Ϊ�գ���notEmpty_��֪ͨ
	notEmpty_.notify_all();

	return Result(sp);
}

void Threadpool::start(int initThreadSize) {
	//��¼��ʼ�̸߳���
	initThreadSize_ = initThreadSize;
	//�����̶߳���
	for (int i = 0; i < initThreadSize_; ++i) {
		auto ptr = std::make_unique<Thread>(std::bind(&Threadpool::threadFunc, this));
		threads_.emplace_back(std::move(ptr));
	}
	//���������߳�
	for (int i = 0; i < initThreadSize_; ++i) {
		threads_[i]->start();
	}
}

//�̺߳��� �̳߳ص������̴߳������������������
void Threadpool::threadFunc() {
	/*std::cout << "begin threadFunc tid : " << std::this_thread::get_id() << std::endl;
	std::cout << "end threadFunc" << std::this_thread::get_id() << std::endl;*/
	for (;;) {
		std::shared_ptr<Task> task;
		{
			std::unique_lock<std::mutex> lock(taskQueMtx_);

			std::cout << "tid : " << std::this_thread::get_id() << "���Ի�ȡ����..."
				<< std::endl;

			notEmpty_.wait(lock, [&]()->bool {return taskQue_.size() > 0; });

			std::cout << "tid : " << std::this_thread::get_id() << "��ȡ����ɹ�..."
				<< std::endl;

			task = taskQue_.front();
			taskQue_.pop();
			taskSize_--;

			//�����Ȼ������������֪ͨ�����߳�ִ������
			if (taskQue_.size() > 0) {
				notEmpty_.notify_all();
			}

			notFull_.notify_all();

		}
		//��ǰ�߳�ִ�л�ȡ������
		if (task != nullptr) {
			task->exec();
		}
	}
}

/* Task��Ա����ʵ�� */

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

/* �̳߳�Ա����ʵ�� */

Thread::Thread(ThreadFunc func) : func_(func){}

Thread::~Thread() {}

void Thread::start() {
	//����һ���߳���ִ��һ���̺߳���
	std::thread t(func_);
	t.detach();
}

/* Result��Ա����ʵ�� */

Result::Result(std::shared_ptr<Task> task, bool isVaild)
	: task_(task),isVaild_(isVaild){
	task_->setResult(this);
}

Any Result::get(){
	if (!isVaild_) {
		return "";
	}
	sem_.wait();
	return std::move(any_);
}

void Result::setVal(Any any) {
	any_ = std::move(any);
	sem_.post();
}
