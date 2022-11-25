/*
*   BSD 3-Clause License, see file labled 'LICENSE' for the full License.
*   Copyright (c) 2022, Peter Ferranti
*   All rights reserved.
*/

#include "threadpool.h"
_SCM_CHILD_DEFINITIONS(ThreadPool)

const ThreadPool::Task ThreadPool::NullTask = std::move([](){});

void ThreadPool::WorkerRun(ThreadPool* pool, Worker* self) {
	self->ID = std::this_thread::get_id();
	pool->m_workerSem->waitFor([&](const int64_t cVal, const int64_t CInitVal){ return (self->Thread != nullptr); });
	pool->m_onTBegin();
	Task T;
	pool->m_workerSem->waitFor([&](const int64_t cVal, const int64_t CInitVal){ return (pool->m_poolState == PoolState::RUNNING); });
	while(pool->m_poolState == PoolState::RUNNING) {
		pool->m_taskSem->waitFor([&](const int64_t cVal, const int64_t CInitVal){ return (!pool->m_tasks.empty()) || (pool->m_poolState != PoolState::RUNNING); });
		if(pool->m_poolState == PoolState::SHUTTING_DOWN) break;
		auto* lock = new std::lock_guard<std::mutex>(*pool->m_taskMTX);
		if(!pool->m_tasks.empty()) {
			T = pool->m_tasks.front();
			pool->m_tasks.pop_front();
			pool->m_taskSem->notify();
			delete lock;
			try {
				T();
			} catch(std::exception& e) {
				// e.what();
			}
		}
		else {
			delete lock;
			T = ThreadPool::NullTask;
		}
	}
	pool->m_onTEnd();
	// {
	// 	std::lock_guard<std::mutex> lock(*pool->m_workerMTX);
	// 	// self->Thread->detach();
	// 	self->Thread = nullptr;
	// 	self->ID = {};
	// 	for(auto itt = begin(pool->m_workers); itt != end(pool->m_workers); itt++) {
	// 		if(&(*itt) == self) { pool->m_workers.erase(itt); break; }
	// 	}
	// }
		pool->m_workerSem->dec();
		pool->m_workerSem->notify();
}

void ThreadPool::AllocateWorker() {
	std::lock_guard<std::mutex> lock(*m_workerMTX);
	if(m_workers.size() < m_maxWorkerCount) {
		m_workers.emplace_back(Worker{{}, nullptr});
		ThreadPool::Worker& newWorker = m_workers.back();
		newWorker.Thread = std::move(new std::thread(ThreadPool::WorkerRun, this, &newWorker));
	}
}

ThreadPool::ThreadPool(int64_t maxWorkerCount, bool isOverflowBlocking, int64_t maxQueueSize, Task onTBegin, Task onTEnd) :
	m_poolState(PoolState::INACTIVE),
	m_maxWorkerCount((maxWorkerCount < 2) ? 2 : maxWorkerCount),
	m_isOverflowBlocking(isOverflowBlocking),
	m_maxQueueSize(maxQueueSize),
	m_tasks(),
	m_workers(),
	m_onTBegin(onTBegin),
	m_onTEnd(onTEnd),
	m_workerSem(new Semaphore),
	m_taskSem(new Semaphore),
	m_workerMTX(new std::mutex),
	m_taskMTX(new std::mutex) {
	for(char i = 0; i < 2; i++)
		AllocateWorker();
	m_poolState = PoolState::RUNNING;
	m_workerSem->notify();
}

	ThreadPool::~ThreadPool() {
		m_poolState = PoolState::SHUTTING_DOWN;
		while(int64_t(*m_workerSem) > 0) {
			m_workerSem->notify();
			m_taskSem->notify();
		}
		delete m_taskSem;
		delete m_workerSem;
	}