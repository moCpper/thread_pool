#include "threadpool.h"

const int TASK_MAX_THRESHHOLD = INT32_MAX;
const int THREAD_MAX_THRESHHOLD = 10;
const int THREAD_MAX_IDLE_TIME = 10; //��λ�� ��

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

	//cachedģʽ��������ȽϽ���������:С���������
	//��Ҫ�������������Ϳ����߳��������ж��Ƿ���Ҫ�����µ��̳߳���
	if (poolMode_ == PoolMode::MODE_CACHED &&
		taskSize_ > idleThreadSize_&&
		curThreadSize_ < threadSizeThreshHold_) {
		//�����µ��̶߳���
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
	//�����̵߳�����״̬
	isPoolRunning_ = true;

	//��¼��ʼ�̸߳���
	initThreadSize_ = initThreadSize;
	curThreadSize_ = initThreadSize;
	//�����̶߳���
	for (int i = 0; i < initThreadSize_; ++i) {
		auto ptr = std::make_unique<Thread>(std::bind(&Threadpool::threadFunc, this,std::placeholders::_1));
		int threadID = ptr->getId();
		threads_.emplace(threadID, std::move(ptr));
		//threads_.emplace_back(std::move(ptr));
	}
	//���������߳�
	for (int i = 0; i < initThreadSize_; ++i) {
		threads_[i]->start();
		idleThreadSize_++;  //��¼�����̵߳�����
	}
}

//�̺߳��� �̳߳ص������̴߳������������������
void Threadpool::threadFunc(int threadId) {  //�̺߳������أ���Ӧ�߳�Ҳ�ͽ�����
	/*std::cout << "begin threadFunc tid : " << std::this_thread::get_id() << std::endl;
	std::cout << "end threadFunc" << std::this_thread::get_id() << std::endl;*/
	auto lastTime = std::chrono::high_resolution_clock().now();

	//�����������ִ����ɣ��̳߳زſ��Ի��������߳���Դ
	for (;;) {
		std::shared_ptr<Task> task;
		{
			std::unique_lock<std::mutex> lock(taskQueMtx_);

			std::cout << "tid : " << std::this_thread::get_id() << "���Ի�ȡ����..."
				<< std::endl;

				//cachedģʽ�£��п����Ѿ�����������̣߳����ǿ���ʱ�䳬��60s,Ӧ�ðѶ�����߳�
				//�������յ�(����initThreadSize_�������߳�Ҫ���л���)
				//��ǰʱ�� - ��һ���߳�ִ�е�ʱ�� > 60s 
			    //�� + ˫���ж�
				while (taskQue_.size() == 0) {
					//�̳߳ؽ����������߳���Դ
					if (!isPoolRunning_) {
						threads_.erase(threadId);
						std::cout << "thread id :" << std::this_thread::get_id() << "exit!" 
							<< std::endl;
						exitCond_.notify_all();
						return;   //�̺߳����������߳̽���
					}

					if (poolMode_ == PoolMode::MODE_CACHED) {
						if (std::cv_status::timeout == notEmpty_.wait_for(lock, std::chrono::seconds(1))) {  //cv_statusΪwait_for����ֵ��ö���ࣩ
							auto now = std::chrono::high_resolution_clock().now();
							auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);
							if (dur.count() >= THREAD_MAX_IDLE_TIME &&
								curThreadSize_ > initThreadSize_) {
								//���ո��߳�
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

		idleThreadSize_++; //����ִ����ɣ������߳�����++;
		lastTime = std::chrono::high_resolution_clock().now();  //�����߳�ִ���������ʱ��
	}
}

bool Threadpool::checkRunningState() const {
	return isPoolRunning_;
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
int Thread::generateId_ = 0;

Thread::Thread(ThreadFunc func) : func_(func),ThreadId_(generateId_++){}

Thread::~Thread() {}

void Thread::start() {
	//����һ���߳���ִ��һ���̺߳���
	std::thread t(func_,ThreadId_);
	t.detach();
}

int Thread::getId() const {
	return ThreadId_;
}

/* Result��Ա����ʵ�� */

Result::Result(std::shared_ptr<Task> task, bool isVaild)
	: task_(task),isVaild_(isVaild){
	task_->setResult(this);
}

Any Result::get(){   //�û�����
	if (!isVaild_) {
		return "";
	}
	sem_.wait();     //���û���������getʱ�����̺߳���δִ����ɣ����������
	return std::move(any_);
}

void Result::setVal(Any any) {   //�̳߳ص��̷߳������
	any_ = std::move(any);
	sem_.post();          //�õ�����ֵ���ź�����Դ++��
}
