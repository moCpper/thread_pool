#include"threadpool.h"
int sum(int a, int b, int c) {
	return a + b + c;
}
int main(){
	Threadpool pool;
	pool.start();
	std::future<int> r = pool.submitTask(sum,2,2,2);
	std::cout << r.get() << std::endl;
	std::future<int> r1 = pool.submitTask(sum, 2, 2, 2);
	std::cout << r1.get() << std::endl;
	std::future<int> r2 = pool.submitTask(sum, 2, 2, 2);
	std::cout << r2.get() << std::endl;

}