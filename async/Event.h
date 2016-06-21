// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_EVENT_H
#define CEPH_MSG_EVENT_H

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

// We use epoll, kqueue, evport, select in descending order by performance.
#if defined(__linux__)
#define HAVE_EPOLL 1
#endif

#if (defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#define HAVE_KQUEUE 1
#endif

#ifdef __sun
#include <sys/feature_tests.h>
#ifdef _DTRACE_VERSION
#define HAVE_EVPORT 1
#endif
#endif

#include <pthread.h>

#include <atomic>
#include <mutex>
#include <condition_variable>

#include "include/utime.h"
#include "include/unordered_map.h"
#include "common/dout.h"
#include "net_handler.h"

#define EVENT_NONE 0
#define EVENT_READABLE 1
#define EVENT_WRITABLE 2

class EventCenter;

class EventCallback {

 public:
  virtual void do_request(int fd_or_id) = 0;
  virtual ~EventCallback() {}       // we want a virtual destructor!!!
};

typedef EventCallback* EventCallbackRef;

struct FiredFileEvent {
  int fd;
  int mask;
};

/*
 * EventDriver is a wrap of event mechanisms depends on different OS.
 * For example, Linux will use epoll(2), BSD will use kqueue(2) and select will
 * be used for worst condition.
 */
class EventDriver {
 public:
  virtual ~EventDriver() {}       // we want a virtual destructor!!!
  virtual int init(EventCenter *center, int nevent) = 0;
  virtual int add_event(int fd, int cur_mask, int mask) = 0;
  virtual int del_event(int fd, int cur_mask, int del_mask) = 0;
  virtual int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) = 0;
  virtual int resize_events(int newsize) = 0;
  virtual bool wakeup_support() { return true; }
};


/*
 * EventCenter maintain a set of file descriptor and handle registered events.
 */
class EventCenter {
  thread_local static unsigned local_id;

  struct FileEvent {
    int mask;
    EventCallbackRef read_cb;
    EventCallbackRef write_cb;
    FileEvent(): mask(0), read_cb(NULL), write_cb(NULL) {}
  };

  struct TimeEvent {
    uint64_t id;
    EventCallbackRef time_cb;

    TimeEvent(): id(0), time_cb(NULL) {}
  };

  /**
     * A Poller object is invoked once each time through the dispatcher's
     * inner polling loop.
     */
 public:
  class Poller {
   public:
    explicit Poller(EventCenter* center, const string& pollerName);
    virtual ~Poller();

    /**
     * This method is defined by a subclass and invoked once by the
     * center during each pass through its inner polling loop.
     *
     * \return
     *      1 means that this poller did useful work during this call.
     *      0 means that the poller found no work to do.
     */
    virtual int poll() = 0;

   private:
    /// The EventCenter object that owns this Poller.  NULL means the
    /// EventCenter has been deleted.
    EventCenter* owner;

    /// Human-readable string name given to the poller to make it
    /// easy to identify for debugging. For most pollers just passing
    /// in the subclass name probably makes sense.
    string poller_name;

    /// Index of this Poller in EventCenter::pollers.  Allows deletion
    /// without having to scan all the entries in pollers. -1 means
    /// this poller isn't currently in EventCenter::pollers (happens
    /// after EventCenter::reset).
    int slot;
  };

  CephContext *cct;
  int nevent;
  // Used only to external event
  std::mutex external_lock;
  std::atomic_ulong external_num_events;
  deque<EventCallbackRef> external_events;
  vector<FileEvent> file_events;
  EventDriver *driver;
  map<utime_t, list<TimeEvent> > time_events;
  // Keeps track of all of the pollers currently defined.  We don't
  // use an intrusive list here because it isn't reentrant: we need
  // to add/remove elements while the center is traversing the list.
  std::vector<Poller*> pollers;
  uint64_t time_event_next_id;
  time_t last_time; // last time process time event
  utime_t next_time; // next wake up time
  int notify_receive_fd;
  int notify_send_fd;
  NetHandler net;
  EventCallbackRef notify_handler;
  unsigned id = 10000;

  int process_time_events();
  FileEvent *_get_file_event(int fd) {
    assert(fd < nevent);
    return &file_events[fd];
  }

 public:
  atomic_t already_wakeup;

  explicit EventCenter(CephContext *c):
    cct(c), nevent(0),
    external_num_events(0),
    driver(NULL), time_event_next_id(1),
    notify_receive_fd(-1), notify_send_fd(-1), net(c),
    notify_handler(NULL),
    already_wakeup(0) {
    last_time = time(NULL);
  }
  ~EventCenter();
  ostream& _event_prefix(std::ostream *_dout);

  int init(int nevent, unsigned idx);
  unsigned get_id() { return id; }

  EventDriver *get_driver() { return driver; }

  // Used by internal thread
  int create_file_event(int fd, int mask, EventCallbackRef ctxt);
  uint64_t create_time_event(uint64_t milliseconds, EventCallbackRef ctxt);
  void delete_file_event(int fd, int mask);
  void delete_time_event(uint64_t id);
  int process_events(int timeout_microseconds);
  void wakeup();

  bool exist_pending_event() const {
    return !external_events.empty() || !time_events.empty();
  }
  // Used by external thread
  void dispatch_event_external(EventCallbackRef e);
  inline bool in_thread() const {
    return local_id == id;
  }
 private:
  template <typename func>
  class C_submit_event : public EventCallback {
    std::mutex lock;
    std::condition_variable cond;
    bool done = false;
    func f;
    bool nonwait;
   public:
    C_submit_event(func &&_f, bool nw)
      : f(std::move(_f)), nonwait(nw) {}
    void do_request(int id) {
      f();
      std::lock_guard<std::mutex> l(lock);
      cond.notify_all();
      done = true;
      if (nonwait)
        delete this;
    }
    void wait() {
      std::unique_lock<std::mutex> l(lock);
      while (!done)
        cond.wait(l);
    }
  };
 public:
  template <typename func>
  void submit_event(func &&f, bool nowait = false) {
    if (in_thread()) {
      f();
      return ;
    }
    if (nowait) {
      C_submit_event<func> *event = new C_submit_event<func>(std::move(f), true);
      dispatch_event_external(event);
    } else {
      C_submit_event<func> event(std::move(f), false);
      dispatch_event_external(&event);
      event.wait();
    }
  };
};


#endif