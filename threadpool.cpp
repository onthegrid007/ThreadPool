/*
*   BSD 3-Clause License, see file labled 'LICENSE' for the full License.
*   Copyright (c) 2022, Peter Ferranti
*   All rights reserved.
*/

#include "threadpool.h"
_SCM_CHILD_DEFINITIONS(ThreadPool)

const ThreadPool::Task ThreadPool::NullTask = std::move([](){});

void ThreadPool::SIGHandler(int sig, siginfo_t* info, void* extra) {
	// BADLOGV(sig);
	ThreadPool* pool = (info->si_ptr ? (ThreadPool*)info->si_ptr : nullptr);
	// BADLOG("caught: " << int(sig));
	switch(sig) {
		case SIGUSR1:
		if(pool) {
			// const __pid_t selfID = gettid();
			// for(auto worker = pool->m_workers.begin(); worker != pool->m_workers.end(); worker++) {
			// 	if(worker->ID == selfID) {
					throw "S";
				// }
			// }
		}
		break;
		case SIGUSR2:
		if(pool) {
			while(pool->m_poolState.load() == PoolState::PAUSED) pause();
		}
		break;
		default:
		break;
	};
}

void ThreadPool::PauseToggle() {
	if(m_poolState == PoolState::RUNNING) {
		m_poolState.store(PoolState::PAUSED);
		m_pauseMTX.lock();
		for(auto& worker : m_workers) {
			sigqueue(worker.ID, SIGUSR2, m_sv);
		}
	}
	else if(m_poolState == PoolState::PAUSED) {
		m_poolState.store(PoolState::RUNNING);
		for(auto& worker : m_workers) {
			sigqueue(worker.ID, SIGPOLL, m_sv);
		}
		sleep_for(seconds(1));
		m_pauseMTX.unlock();
		if(m_missedTaskEnqueues) {
			while(m_missedTaskEnqueues) {
				m_taskSem.notify_one();
				m_missedTaskEnqueues--;
			}
		}
	}
}

void ThreadPool::AllocateWorker() {
	if((m_workers.size() < m_maxWorkerCount) && (m_poolState == PoolState::RUNNING)) {
		std::lock_guard<std::mutex> lock(m_workerMTX);
		if(m_workers.size() >= m_maxWorkerCount) return;
		m_workers.emplace_back();
		m_workers.back().Thread = std::make_unique<std::thread>([=](Worker* self){
			sigjmp_buf RunTask, SigChk;
			self->ID = gettid();
			// BADLOGV(int64_t(self->ID));
			if(sigaction(SIGUSR1, &m_siga, NULL) != 0) throw std::runtime_error("Unable to set SIGUSR1 handler on new worker!");
			if(sigaction(SIGUSR2, &m_siga, NULL) != 0) throw std::runtime_error("Unable to set SIGUSR2 handler on new worker!");
			if(sigaction(SIGPOLL, &m_siga, NULL) != 0) throw std::runtime_error("Unable to set SIGPOLL handler on new worker!");
			m_onTBegin();
			Task T;
			while((m_poolState.load() != PoolState::SHUTTING_DOWN) && (m_poolState.load() != PoolState::INACTIVE)) {
				if(sigsetjmp(RunTask, 1)) {
					try {
						T();
					} catch(const char* e) {
						// std::string(e.what()) == "S" ? longjmp(SigChk, 0) : void();
					}
				}
				sigsetjmp(SigChk, 1);
				BADLOGV(int(m_poolState.load()));
				try {
					m_taskSem.waitFor([=](const int64_t cVal, const int64_t cInitVal){ return (!m_tasks.empty()) || (m_poolState.load() == PoolState::SHUTTING_DOWN) || (m_poolState.load() == PoolState::INACTIVE); });
					if((m_poolState == PoolState::SHUTTING_DOWN) || (m_poolState == PoolState::INACTIVE)) break;
					std::lock_guard<std::mutex> lock(m_taskMTX);
					if(!m_tasks.empty()) {
						T = std::move(m_tasks.front());
						m_tasks.pop_front();
						throw "T";
					}
				} catch(const char* e) {
					std::string(e) == "T" ? siglongjmp(RunTask, 1) : void();
					std::string(e) == "S" ? siglongjmp(SigChk, 0) : void();
				}
			}
		}, &m_workers.back());
	}
}

ThreadPool::ThreadPool(int64_t maxWorkerCount, bool isOverflowBlocking, int64_t maxQueueSize, Task onTBegin, Task onTEnd) :
	m_poolState(PoolState::RUNNING),
	m_maxWorkerCount((maxWorkerCount < 2) ? 2 : maxWorkerCount),
	m_isOverflowBlocking(isOverflowBlocking),
	m_maxQueueSize(maxQueueSize),
	m_tasks(),
	m_workers(),
	m_onTBegin(onTBegin),
	m_onTEnd(onTEnd),
	m_workerSem(0),
	m_taskSem(0),
	m_workerMTX(),
	m_taskMTX(),
	m_sv({.sival_ptr = (void*)this}) {
	for(char i = 0; i < 2; i++)
		AllocateWorker();
}

ThreadPool::~ThreadPool() {
	m_poolState.store(PoolState::SHUTTING_DOWN);
	if(m_poolState == PoolState::PAUSED) {
		m_paused.store(false);
		for(auto& worker : m_workers) {
			sigqueue(worker.ID, SIGPOLL, m_sv);
		}
		sleep_for(seconds(1));
	}
	for(auto& worker : m_workers) {
		while(!(worker.Thread.get()->joinable())) {
			m_taskSem.notify();
			sigqueue(worker.ID, SIGUSR1, m_sv);
		}
		m_taskSem.notify();
		worker.Thread.get()->join();
	}
}