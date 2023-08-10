/*
*   BSD 3-Clause License, see file labled 'LICENSE' for the full License.
*   Copyright (c) 2023, Peter Ferranti
*   All rights reserved.
*/

#ifndef THREADPOOL_HPP_
#define THREADPOOL_HPP_
#include <signal.h>
#include <setjmp.h>
#include <string.h>
#include <thread>
#include <deque>
#include <vector>
#include <functional>
#include <future>
#include <utility>
#include <stdexcept>
#include <type_traits>

#include "vendor/PlatformDetection/PlatformDetection.h"
#include "vendor/ADVClock/vendor/Timestamp/timestamp.hpp"
#include "vendor/Semaphore/semaphore.hpp"

using namespace std::this_thread;
using namespace std::chrono;

class PlatformThread : public std::thread {
	public:
	template<typename... Args>
	PlatformThread(Args... args) : std::thread(args...), vState(VolitileState::RUNNING) {}
	enum VolitileState : std::int8_t {
		IDLE = 0,
		RUNNING,
		PAUSED,
		DEAD
	};
	std::atomic<VolitileState> vState;
	#ifdef _BUILD_PLATFORM_LINUX
	public:
	typedef __pid_t IDType;
	static IDType ID() { return gettid(); }
	#elif _BUILD_PLATFORM_WINDOWS
	public:
	typedef std::thread::id IDType;
	IDType ID() { return getThreadID(); }
	#endif
	
};

class ThreadPool : public NonCopyable {
	public:
	typedef std::function<void(void)> TaskType;
	typedef size_t SizeType;
	enum PoolState : std::int8_t {
		RUNNING = 0,
		PAUSED = 1,
		SHUTTING_DOWN = 2,
		INACTIVE = -1
	};
	enum EnqueuePriority : bool { DEFAULT = false, EXPEDITED };
	enum QueueProcedure : bool { NONBLOCKING = false, BLOCKING };
	
	void TogglePause() {
		if(m_poolState == PoolState::RUNNING) {
			m_poolState.store(PoolState::PAUSED);
			m_pauseMTX.lock();
			for(auto& worker : m_workers)
				if(worker.ID)
					sigqueue(worker.ID, SIGUSR2, m_sv);
		}
		else if(m_poolState == PoolState::PAUSED) {
			m_poolState.store(PoolState::RUNNING);
			m_pauseMTX.unlock();
			for(auto& worker : m_workers) {
				if(worker.ID)
					sigqueue(worker.ID, SIGPOLL, m_sv);
			}
			sleep_for(seconds(1));
			if(m_missedTaskEnqueues) {
				while(m_missedTaskEnqueues) {
					// fix to overflow block here
					m_taskSem.spinOne();
					m_missedTaskEnqueues--;
				}
			}
		}
	}
	
	void ToggleQueueProcedure() {
		// m_queueProcedure = !m_queueProcedure;
	}
	
	private:
	static void SIGHandler(int sig, siginfo_t* info, void* extra) {
		const ThreadPool* pool = (info->si_ptr ? (ThreadPool*)info->si_ptr : nullptr);
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
	
	typedef struct Worker_t {
		PlatformThread* Thread;
		PlatformThread::IDType ID = 0;
		Semaphore Sem;
		Timestamp Begin;
	} WorkerType;

	void Decay() {
		std::lock_guard<std::mutex> lock(m_workerMTX);
		auto workerItt(m_workers.begin());
		while(workerItt != m_workers.end()) {
			if((*workerItt).Thread->vState == PlatformThread::VolitileState::DEAD) {
				delete (*workerItt).Thread;
				m_workers.erase(workerItt);
			}
			else {
				workerItt++;
			}
		}
	}
	
	void AllocateWorker() {
		static const TaskType NullTask{[](){}};
		if((m_workers.size() < m_workerLimit) && (m_poolState == PoolState::RUNNING)) {
			std::lock_guard<std::mutex> lock(m_workerMTX);
			if(m_workers.size() >= m_workerLimit) return;
			m_workers.emplace_back(); //nullptr, 0, {0}, Timestamp{});
			WorkerType* newW{&m_workers.back()};
			Semaphore idLock{0};
			auto workerTask{[&](WorkerType* self){
				TaskType T{NullTask};
				sigjmp_buf RunTask, SigChk;
				self->ID = PlatformThread::ID();
				idLock.inc();
				if(sigaction(SIGUSR1, &m_siga, NULL) != 0) throw std::runtime_error("Unable to set SIGUSR1 handler on new worker!");
				if(sigaction(SIGUSR2, &m_siga, NULL) != 0) throw std::runtime_error("Unable to set SIGUSR2 handler on new worker!");
				if(sigaction(SIGPOLL, &m_siga, NULL) != 0) throw std::runtime_error("Unable to set SIGPOLL handler on new worker!");
				while((m_poolState.load() != PoolState::SHUTTING_DOWN) && (m_poolState.load() != PoolState::INACTIVE)) {
					if(sigsetjmp(RunTask, 1)) {
						try {
							T();
						} catch(const char* e) {
							// std::string(e.what()) == "S" ? longjmp(SigChk, 0) : void();in::
						}
					}
					sigsetjmp(SigChk, 1);
					try {
						const auto until{Timestamp::ClockType::now() + seconds(m_decayTimeout)};
						self->Thread->vState = PlatformThread::VolitileState::IDLE;
						if(m_taskSem.waitFor([&](const int64_t cVal, const int64_t cInitVal){ return (!m_tasks.empty()) || (m_poolState.load() == PoolState::SHUTTING_DOWN) || (m_poolState.load() == PoolState::INACTIVE); }, until) == std::cv_status::timeout) {
							Decay();
							break;
						}
						if((m_poolState.load() == PoolState::SHUTTING_DOWN) || (m_poolState.load() == PoolState::INACTIVE)) break;
						self->Thread->vState = PlatformThread::VolitileState::RUNNING;
						std::lock_guard<std::mutex> lock(m_taskMTX);
						if(!m_tasks.empty()) {
							T = m_tasks.front();
							m_tasks.pop_front();
							throw "T";
						}
					} catch(const char* e) {
						!(strcmp(e, "T")) ? siglongjmp(RunTask, 1) : void();
						!(strcmp(e, "S")) ? siglongjmp(SigChk, 0) : void();
					}
				}
				self->ID = PlatformThread::IDType();
				self->Thread->vState = PlatformThread::VolitileState::DEAD;
			}};
			newW->Thread = new PlatformThread(workerTask, newW);
			idLock.waitForC(1);
		}
	}
	
	SizeType m_workerLimit;
	std::atomic<QueueProcedure> m_queueProcedure;
	SizeType m_queueLimit;
	std::deque<TaskType> m_tasks;
	std::deque<WorkerType> m_workers;
	Timestamp m_begin;
	Semaphore m_workerSem;
	Semaphore m_taskSem;
	std::mutex m_workerMTX;
	std::mutex m_taskMTX;
	std::mutex m_pauseMTX;
	std::atomic<bool> m_paused;
	std::atomic<PoolState> m_poolState;
	static constexpr struct sigaction m_siga{.sa_sigaction = ThreadPool::SIGHandler, .sa_mask = {}, .sa_flags = SA_SIGINFO | SA_NODEFER, .sa_restorer = nullptr};
	const __sigval_t m_sv;
	std::atomic<SizeType> m_missedTaskEnqueues; 
	std::uint8_t m_decayTimeout{30};// }
	
	public:
	ThreadPool(SizeType workerLimit, QueueProcedure qProcedure, SizeType queueLimit) :
		m_workerLimit((workerLimit < 2) ? 2 : workerLimit),
		m_queueProcedure(qProcedure),
		m_queueLimit(queueLimit),
		m_workerSem(0),
		m_taskSem(0),
		m_paused(false),
		m_poolState(PoolState::RUNNING),
		m_sv({.sival_ptr = static_cast<void*>(this)}) {
		for(char i = 0; i < 2; i++)
			AllocateWorker();
	}
	
	~ThreadPool() {
		if(m_poolState == PoolState::PAUSED) {
			m_poolState.store(PoolState::SHUTTING_DOWN);
			m_pauseMTX.unlock();
			m_paused.store(false);
			for(auto& worker : m_workers)
				if(worker.ID)
					sigqueue(worker.ID, SIGPOLL, m_sv);
			sleep_for(seconds(1));
		}
		else {
			m_poolState.store(PoolState::SHUTTING_DOWN);
		}
		for(auto& worker : m_workers) {
			while(!(worker.Thread->joinable())) {
				m_taskSem.spinAll();
				if(worker.ID)
					sigqueue(worker.ID, SIGUSR1, m_sv);
			}
			m_taskSem.spinAll();
			worker.Thread->join();
			delete worker.Thread;
		}
		
	}
	
	template<typename F, typename... Args, typename RTN_T = typename std::invoke_result<F, Args...>::type>
	auto enqueue_work(F f, Args... args, EnqueuePriority&& priority) -> std::future<RTN_T> {
		auto tprom{std::make_shared<std::promise<RTN_T>>()};
		auto rtn{tprom->get_future()};
		if(m_poolState != PoolState::INACTIVE) {
			if(m_workers.size() < m_workerLimit) AllocateWorker();
			while(true) {
				if((m_tasks.size() >= m_queueLimit) && (m_queueProcedure == QueueProcedure::BLOCKING)) {
					std::lock_guard<std::mutex> lock(m_pauseMTX);
					m_taskSem.waitFor([&](const SizeType cVal, const SizeType CInitVal){ return (m_tasks.size() < m_queueLimit); });
				}
				m_taskMTX.lock();
				if(m_tasks.size() < m_queueLimit) {
					std::function<RTN_T()> tfunc = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
					TaskType etask{[&, tprom, tfunc](){
						try {
							if constexpr(std::is_void<RTN_T>::value) {
								tfunc();
								tprom->set_value();
							}
							else {
								tprom->set_value(tfunc());
							}
						}
						catch (...) {
							try {
								tprom->set_exception(std::current_exception());
							} catch (...) {}
						}
					}};
					if(priority) {
						m_tasks.emplace_front(etask);
					}
					else {
						m_tasks.emplace_back(etask);
					}
					m_taskMTX.unlock();
					if(m_paused.load()) { m_missedTaskEnqueues++; } else { m_taskSem.spinOne(); }
					break;
				}
				else {
					m_taskMTX.unlock();
				}
			}
		}
		return rtn;
	}
};
#endif
