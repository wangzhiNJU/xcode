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

#include <time.h>

#include "common/errno.h"
#include "Event.h"

#ifdef HAVE_DPDK
#include "msg/async/dpdk/EventDPDK.h"
#endif

#ifdef HAVE_EPOLL
#include "EventEpoll.h"
#else
#ifdef HAVE_KQUEUE
#include "EventKqueue.h"
#else
#include "EventSelect.h"
#endif
#endif

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "EventCallback "
class C_handle_notify : public EventCallback {
  EventCenter *center;
  CephContext *cct;

 public:
  C_handle_notify(EventCenter *c, CephContext *cc): center(c), cct(cc) {}
  void do_request(int fd_or_id) {
    char c[256];
    do {
      center->already_wakeup.set(0);
      int r = read(fd_or_id, c, sizeof(c));
      if (r < 0) {
        ldout(cct, 1) << __func__ << " read notify pipe failed: " << cpp_strerror(errno) << dendl;
        break;
      }
    } while (center->already_wakeup.read());
  }
};

#undef dout_prefix
#define dout_prefix _event_prefix(_dout)

/**
 * Construct a Poller.
 *
 * \param center
 *      EventCenter object through which the poller will be invoked (defaults
 *      to the global #RAMCloud::center object).
 * \param pollerName
 *      Human readable name that can be printed out in debugging messages
 *      about the poller. The name of the superclass is probably sufficient
 *      for most cases.
 */
EventCenter::Poller::Poller(EventCenter* center, const string& name)
    : owner(center), poller_name(name), slot(owner->pollers.size())
{
  owner->pollers.push_back(this);
}

/**
 * Destroy a Poller.
 */
EventCenter::Poller::~Poller()
{
  // Erase this Poller from the vector by overwriting it with the
  // poller that used to be the last one in the vector.
  //
  // Note: this approach is reentrant (it is safe to delete a
  // poller from a poller callback, which means that the poll
  // method is in the middle of scanning the list of all pollers;
  // the worst that will happen is that the poller that got moved
  // may not be invoked in the current scan).
  owner->pollers[slot] = owner->pollers.back();
  owner->pollers[slot]->slot = slot;
  owner->pollers.pop_back();
  slot = -1;
}

ostream& EventCenter::_event_prefix(std::ostream *_dout)
{
  return *_dout << "Event(" << this << " id=" << get_id() << " nevent=" << nevent
                << " time_id=" << time_event_next_id << ").";
}

thread_local unsigned EventCenter::local_id = 10000;

int EventCenter::init(int n, unsigned idx)
{
  // can't init multi times
  assert(nevent == 0);

  local_id = id = idx;

  driver = nullptr;
  if (cct->_conf->ms_async_transport_type == "dpdk") {
#ifdef HAVE_DPDK
    driver = new DPDKDriver(cct);
#endif
  } else {
#ifdef HAVE_EPOLL
  driver = new EpollDriver(cct);
#else
#ifdef HAVE_KQUEUE
  driver = new KqueueDriver(cct);
#else
  driver = new SelectDriver(cct);
#endif
#endif
  }

  if (!driver) {
    lderr(cct) << __func__ << " failed to create event driver " << dendl;
    return -1;
  }

  int r = driver->init(this, n);
  if (r < 0) {
    lderr(cct) << __func__ << " failed to init event driver." << dendl;
    return r;
  }

  file_events.resize(n);
  nevent = n;

  if (!driver->wakeup_support())
    return 0;

  int fds[2];
  if (pipe(fds) < 0) {
    lderr(cct) << __func__ << " can't create notify pipe" << dendl;
    return -errno;
  }

  notify_receive_fd = fds[0];
  notify_send_fd = fds[1];
  r = net.set_nonblock(notify_receive_fd);
  if (r < 0) {
    return r;
  }
  r = net.set_nonblock(notify_send_fd);
  if (r < 0) {
    return r;
  }
  notify_handler = new C_handle_notify(this, cct),
  r = create_file_event(notify_receive_fd, EVENT_READABLE, notify_handler);
  return r;
}

EventCenter::~EventCenter()
{
  {
    std::lock_guard<std::mutex> l(external_lock);
    while (!external_events.empty()) {
      EventCallbackRef e = external_events.front();
      if (e)
        e->do_request(0);
      external_events.pop_front();
    }
  }
  assert(time_events.empty());

  if (notify_receive_fd >= 0)
    ::close(notify_receive_fd);
  if (notify_send_fd >= 0)
    ::close(notify_send_fd);

  delete driver;
  delete notify_handler;
}

int EventCenter::create_file_event(int fd, int mask, EventCallbackRef ctxt)
{
  assert(in_thread());
  int r = 0;
  if (fd >= nevent) {
    int new_size = nevent << 2;
    while (fd > new_size)
      new_size <<= 2;
    ldout(cct, 10) << __func__ << " event count exceed " << nevent << ", expand to " << new_size << dendl;
    r = driver->resize_events(new_size);
    if (r < 0) {
      lderr(cct) << __func__ << " event count is exceed." << dendl;
      return -ERANGE;
    }
    file_events.resize(new_size);
    nevent = new_size;
  }

  EventCenter::FileEvent *event = _get_file_event(fd);
  ldout(cct, 20) << __func__ << " create event started fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
  if (event->mask == mask)
    return 0;

  r = driver->add_event(fd, event->mask, mask);
  if (r < 0) {
    // Actually we don't allow any failed error code, caller doesn't prepare to
    // handle error status. So now we need to assert failure here. In practice,
    // add_event shouldn't report error, otherwise it must be a innermost bug!
    assert(0 == "BUG!");
    return r;
  }

  event->mask |= mask;
  if (mask & EVENT_READABLE) {
    event->read_cb = ctxt;
  }
  if (mask & EVENT_WRITABLE) {
    event->write_cb = ctxt;
  }
  ldout(cct, 10) << __func__ << " create event end fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
  return 0;
}

void EventCenter::delete_file_event(int fd, int mask)
{
  assert(fd >= 0);
  assert(in_thread());
  if (fd >= nevent) {
    ldout(cct, 1) << __func__ << " delete event fd=" << fd << " is equal or greater than nevent=" << nevent
                  << "mask=" << mask << dendl;
    return ;
  }
  EventCenter::FileEvent *event = _get_file_event(fd);
  ldout(cct, 20) << __func__ << " delete event started fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
  if (!event->mask)
    return ;

  int r = driver->del_event(fd, event->mask, mask);
  if (r < 0) {
    // see create_file_event
    assert(0 == "BUG!");
  }

  if (mask & EVENT_READABLE && event->read_cb) {
    event->read_cb = nullptr;
  }
  if (mask & EVENT_WRITABLE && event->write_cb) {
    event->write_cb = nullptr;
  }

  event->mask = event->mask & (~mask);
  ldout(cct, 10) << __func__ << " delete event end fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
}

uint64_t EventCenter::create_time_event(uint64_t microseconds, EventCallbackRef ctxt)
{
  assert(in_thread());
  uint64_t id = time_event_next_id++;

  ldout(cct, 10) << __func__ << " id=" << id << " trigger after " << microseconds << "us"<< dendl;
  EventCenter::TimeEvent event;
  utime_t expire;
  struct timeval tv;

  if (microseconds < 5) {
    tv.tv_sec = 0;
    tv.tv_usec = microseconds;
  } else {
    expire = ceph_clock_now(cct);
    expire.copy_to_timeval(&tv);
    tv.tv_sec += microseconds / 1000000;
    tv.tv_usec += microseconds % 1000000;
  }
  expire.set_from_timeval(&tv);

  event.id = id;
  event.time_cb = ctxt;
  time_events[expire].push_back(event);
  if (expire < next_time)
    wakeup();

  return id;
}

// TODO: Ineffective implementation now!
void EventCenter::delete_time_event(uint64_t id)
{
  assert(in_thread());
  ldout(cct, 10) << __func__ << " id=" << id << dendl;
  if (id >= time_event_next_id)
    return ;

  for (map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
       it != time_events.end(); ++it) {
    for (list<TimeEvent>::iterator j = it->second.begin();
         j != it->second.end(); ++j) {
      if (j->id == id) {
        it->second.erase(j);
        if (it->second.empty())
          time_events.erase(it);
        return ;
      }
    }
  }
}

void EventCenter::wakeup()
{
  // No need to wake up since we never sleep
  if (!pollers.empty() || !driver->wakeup_support())
    return ;

  if (already_wakeup.compare_and_swap(0, 1)) {
    ldout(cct, 1) << __func__ << dendl;
    char buf[1];
    buf[0] = 'c';
    // wake up "event_wait"
    int n = write(notify_send_fd, buf, 1);
    // FIXME ?
    assert(n == 1);
  }
}

int EventCenter::process_time_events()
{
  int processed = 0;
  time_t now = time(NULL);
  utime_t cur = ceph_clock_now(cct);
  ldout(cct, 10) << __func__ << " cur time is " << cur << dendl;

  /* If the system clock is moved to the future, and then set back to the
   * right value, time events may be delayed in a random way. Often this
   * means that scheduled operations will not be performed soon enough.
   *
   * Here we try to detect system clock skews, and force all the time
   * events to be processed ASAP when this happens: the idea is that
   * processing events earlier is less dangerous than delaying them
   * indefinitely, and practice suggests it is. */
  if (now < last_time) {
    map<utime_t, list<TimeEvent> > changed;
    for (map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
         it != time_events.end(); ++it) {
      changed[utime_t()].swap(it->second);
    }
    time_events.swap(changed);
  }
  last_time = now;

  map<utime_t, list<TimeEvent> >::iterator prev;
  list<TimeEvent> need_process;
  for (map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
       it != time_events.end(); ) {
    prev = it;
    if (cur >= it->first) {
      need_process.splice(need_process.end(), it->second);
      ++it;
      time_events.erase(prev);
    } else {
      break;
    }
  }

  for (list<TimeEvent>::iterator it = need_process.begin();
       it != need_process.end(); ++it) {
    ldout(cct, 10) << __func__ << " process time event: id=" << it->id << dendl;
    it->time_cb->do_request(it->id);
    processed++;
  }

  return processed;
}

int EventCenter::process_events(int timeout_microseconds)
{
  struct timeval tv;
  int numevents;
  bool trigger_time = false;
  utime_t now = ceph_clock_now(cct);;

  bool blocking = pollers.empty() && !external_num_events.load();
  // If exists external events or exists poller, don't block
  if (blocking) {
    utime_t period, shortest;
    now.copy_to_timeval(&tv);
    if (timeout_microseconds > 0) {
      tv.tv_sec += timeout_microseconds / 1000000;
      tv.tv_usec += timeout_microseconds % 1000000;
    }
    shortest.set_from_timeval(&tv);

    map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
    if (it != time_events.end() && shortest >= it->first) {
      ldout(cct, 10) << __func__ << " shortest is " << shortest << " it->first is " << it->first << dendl;
      shortest = it->first;
      trigger_time = true;
      if (shortest > now) {
        period = shortest - now;
        period.copy_to_timeval(&tv);
      } else {
        tv.tv_sec = 0;
        tv.tv_usec = 0;
      }
    } else {
      tv.tv_sec = timeout_microseconds / 1000000;
      tv.tv_usec = timeout_microseconds % 1000000;
    }
    next_time = shortest;
    ldout(cct, 10) << __func__ << " wait second " << tv.tv_sec << " usec " << tv.tv_usec << dendl;
    next_time = shortest;
  } else {
    map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
    if (it != time_events.end() && now >= it->first)
      trigger_time = true;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    next_time = now;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
  }

  vector<FiredFileEvent> fired_events;
  numevents = driver->event_wait(fired_events, &tv);
  for (int j = 0; j < numevents; j++) {
    int rfired = 0;
    FileEvent *event;
    EventCallbackRef cb;
    event = _get_file_event(fired_events[j].fd);

    /* note the event->mask & mask & ... code: maybe an already processed
    * event removed an element that fired and we still didn't
    * processed, so we check if the event is still valid. */
    if (event->mask & fired_events[j].mask & EVENT_READABLE) {
      rfired = 1;
      cb = event->read_cb;
      cb->do_request(fired_events[j].fd);
    }

    if (event->mask & fired_events[j].mask & EVENT_WRITABLE) {
      if (!rfired || event->read_cb != event->write_cb) {
        cb = event->write_cb;
        cb->do_request(fired_events[j].fd);
      }
    }

    ldout(cct, 20) << __func__ << " event_wq process is " << fired_events[j].fd << " mask is " << fired_events[j].mask << dendl;
  }

  if (trigger_time)
    numevents += process_time_events();

  if (external_num_events.load()) {
    external_lock.lock();
    if (external_events.empty()) {
      external_lock.unlock();
    } else {
      deque<EventCallbackRef> cur_process;
      cur_process.swap(external_events);
      external_num_events.store(0);
      external_lock.unlock();
      while (!cur_process.empty()) {
        EventCallbackRef e = cur_process.front();
        if (e)
          e->do_request(0);
        cur_process.pop_front();
      }
    }
  }

  if (!blocking) {
    for (uint32_t i = 0; i < pollers.size(); i++)
      numevents += pollers[i]->poll();
  }

  if (numevents == 0)
    _mm_pause();

  return numevents;
}

void EventCenter::dispatch_event_external(EventCallbackRef e)
{
//  ldout(cct, 20) << __func__ << " event=" << e << dendl;//wangzhi
  ldout(cct, 20) << __func__ << " event=" << e << " [" << local_id << ", " << id << "]" << dendl;
  external_lock.lock();
  external_events.push_back(e);
  ++external_num_events;
  external_lock.unlock();
  if (local_id != id)
    wakeup();
}
