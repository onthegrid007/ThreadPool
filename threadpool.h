/*
*   BSD 3-Clause License, see file labled 'LICENSE' for the full License.
*   Copyright (c) 2022, Peter Ferranti
*   All rights reserved.
*/

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <thread>
#include <deque>
#include <vector>

#define BADLOG(x) std::cout << x << std::endl;
#define BADLOGV(x) BADLOG(#x << ":") BADLOG(x << std::endl)

#include "vendor/Semaphore/vendor/Singleton/singleton_container_map.hpp"
#include "vendor/Timestamp/timestamp.hpp"
#include "vendor/Semaphore/semaphore.h"

using namespace std::this_thread;
using namespace std::chrono;

class ThreadPool : public SingletonContainerMap<ThreadPool> {
	public:
	typedef std::function<void(void)> Task;
	typedef std::thread::id TIDType;
	enum PoolState : char {
		RUNNING = 0,
		SHUTTING_DOWN = 1,
		INACTIVE = -1
	};
	
	private:
	_SCM_CHILD_DECLORATIONS(ThreadPool)
	typedef struct Worker_t {
		TIDType ID;
		std::thread* Thread = nullptr;
		Timestamp Begin;
	} Worker;
	static const Task NullTask;
	Task m_onTBegin = NullTask;
	Task m_onTEnd = NullTask;
	static void WorkerRun(ThreadPool* pool, ThreadPool::Worker* self);
	void AllocateWorker();
	int64_t m_maxWorkerCount = 2;
	int64_t m_maxQueueSize = 32;
	std::deque<Task> m_tasks = {};
	std::vector<Worker> m_workers = {};
	Timestamp m_begin;
	Semaphore* m_workerSem = nullptr;
	Semaphore* m_taskSem = nullptr;
	std::mutex* m_workerMTX = nullptr;
	std::mutex* m_taskMTX = nullptr;
	PoolState m_poolState = PoolState::INACTIVE;
	bool m_isOverflowBlocking = true;
	ThreadPool(int64_t maxWorkerCount = 2, bool isOverflowBlocking = true, int64_t maxQueueSize = 32, Task onTBegin = NullTask, Task onTEnd = NullTask);
	~ThreadPool();
	
	public:
	template<typename Func, typename... Args>
	ThreadPool& enqueue_work(Func&& F, Args&& ... args) {
		if(m_poolState != PoolState::INACTIVE) {
			if(m_workers.size() < m_maxWorkerCount) AllocateWorker();
			try_enqueue:
			if((m_tasks.size() >= m_maxQueueSize) && m_isOverflowBlocking) m_taskSem->waitFor([&](const int64_t cVal, const int64_t CInitVal){ return (m_tasks.size() < m_maxQueueSize); });
			
		auto* lock = new std::lock_guard<std::mutex>(*m_taskMTX);
				if(m_tasks.size() < m_maxQueueSize) {
					m_tasks.emplace_back(std::move([&](){ F(args...); }));
					delete lock;
					m_taskSem->notify();
				}
				else {
					delete lock;
					goto try_enqueue;
				}
			}
		// }
		return *this;
	}
};
#endif
