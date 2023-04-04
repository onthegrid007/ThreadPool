/*
*   BSD 3-Clause License, see file labled 'LICENSE' for the full License.
*   Copyright (c) 2022, Peter Ferranti
*   All rights reserved.
*/

#ifndef THREADPOOL_H_
#define THREADPOOL_H_
#include <signal.h>
#include <setjmp.h>
#include <thread>
#include <deque>
#include <vector>
#include <functional>
#include <future>
#include <utility>
#include <stdexcept>
#include <type_traits>

#include "vendor/Semaphore/vendor/Singleton/singleton_container_map.hpp"
#include "vendor/ADVClock/vendor/Timestamp/timestamp.hpp"
#include "vendor/Semaphore/semaphore.h"

#define BADLOG(x) std::cout << x << std::endl;
#define BADLOGV(x) BADLOG(#x << ":") BADLOG((x) << std::endl)

using namespace std::this_thread;
using namespace std::chrono;

class ThreadPool : public SingletonContainerMap<ThreadPool> {
	public:
	typedef std::function<void(void)> Task;
	typedef std::thread::id TIDType;
	typedef __pid_t PIDType;
	enum PoolState : char {
		RUNNING = 0,
		PAUSED = 1,
		SHUTTING_DOWN = 2,
		INACTIVE = -1
	};
	void PauseToggle();
	
	private:
	_SCM_CHILD_DECLORATIONS(ThreadPool)
	static void SIGHandler(int sig, siginfo_t* info, void* extra);
	typedef struct Worker_t {
		std::unique_ptr<std::thread> Thread;
		PIDType ID;
		Timestamp Begin;
	} Worker;
	static const Task NullTask;
	Task m_onTBegin = NullTask;
	Task m_onTEnd = NullTask;
	void AllocateWorker();
	int64_t m_maxWorkerCount = 2;
	int64_t m_maxQueueSize = 32;
	std::deque<Task> m_tasks = {};
	std::vector<Worker> m_workers;
	Timestamp m_begin;
	Semaphore m_workerSem;
	Semaphore m_taskSem;
	std::mutex m_workerMTX;
	std::mutex m_taskMTX;
	std::mutex m_pauseMTX;
	std::atomic<bool> m_paused; // = PoolState::INACTIVE;
	std::atomic<PoolState> m_poolState; // = PoolState::INACTIVE;
	static constexpr struct sigaction m_siga = {.sa_sigaction = ThreadPool::SIGHandler, .sa_mask = {}, .sa_flags = SA_SIGINFO | SA_NODEFER, .sa_restorer = nullptr};
	const __sigval_t m_sv;
	bool m_isOverflowBlocking = true;
	std::atomic<int> m_missedTaskEnqueues; // = 0;
	
	ThreadPool(int64_t maxWorkerCount = 2, bool isOverflowBlocking = true, int64_t maxQueueSize = 32, Task onTBegin = NullTask, Task onTEnd = NullTask);
	~ThreadPool();
	
	public:
	template<typename F, typename... Args, typename RTN_T = std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>>
	auto enqueue_work(F f, Args... args) -> std::future<RTN_T> {
		auto tprom = std::make_shared<std::promise<RTN_T>>();
		if(m_poolState != PoolState::INACTIVE) {
			jmp_buf trySemWait;
			if(m_workers.size() < m_maxWorkerCount) AllocateWorker();
			std::function<RTN_T(Args...)> tfunc = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
			// try_enqueue:
			setjmp(trySemWait);
			if((m_tasks.size() >= m_maxQueueSize) && m_isOverflowBlocking) {
				std::lock_guard<std::mutex> lock(m_pauseMTX);
				m_taskSem.waitFor([&](const int64_t cVal, const int64_t CInitVal){ return (m_tasks.size() < m_maxQueueSize); });
			}
			
			m_taskMTX.lock();
			if(m_tasks.size() < m_maxQueueSize) {
				m_tasks.emplace_back([tfunc, tprom](){
					try {
						if constexpr(std::is_void_v<RTN_T>) {
							std::invoke(tfunc);
							tprom->set_value();
						}
						else {
							tprom->set_value(std::invoke(tfunc));
						}
					}
					catch (...) {
						try {
							tprom->set_exception(std::current_exception());
						} catch (...) {}
					}
            	});
				m_taskMTX.unlock();
				if(m_paused.load()) { m_missedTaskEnqueues++; } else { m_taskSem.notify_one(); }
			}
			else {
				m_taskMTX.unlock();
				longjmp(trySemWait, 0); // try_enqueue;
			}
		}
		return tprom->get_future();
	}
};
#endif
