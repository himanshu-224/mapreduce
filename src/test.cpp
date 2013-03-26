#include <iostream>
#include <utility>
#include <thread>
#include <chrono>
#include <functional>
#include <atomic>
 
 using namespace std;
void f1()
{
    int n=1;
    for (int i = 0; i < 5; ++i) {
        std::cout << "Thread " << n << " executing\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
 
void f2(int& n)
{
    for (int i = 0; i < 5; ++i) {
        std::cout << "Thread 2 executing\n";
        ++n;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
 
class newClass{
private :
    thread t1,t2,t3,t4;
    
public:
    newClass()
    {
            int n = 0;
    t2=thread(f1); // pass by value
    t3=thread(f2, std::ref(n)); // pass by reference
    t4=thread(std::move(t3)); // t4 is now running f2(). t3 is no longer a thread
    std::cout << "Final value of n is " << n << '\n';
    }
    ~newClass()
    {
        t2.join();
        t4.join();
    }
};
int main()
{
newClass obj= newClass();
}