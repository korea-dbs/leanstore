#pragma once

#include <atomic>
#include <cassert>
#include <climits>
#include <cstdlib>
#include <format>
#include <limits>
#include <memory>

#include "range_lock/node.h"

template <typename T, unsigned maxLevel = 16>
class ConcurrentRangeLock {
 private:
  std::atomic<size_t> elementsCount{0};

  int randomLevel();
  bool findInsert(T start, T end, Node<T> **preds, Node<T> **succs);
  bool findExact(T start, T end, Node<T> **preds, Node<T> **succs);
  void findDelete(T start);

 public:
  Node<T> *tail;
  Node<T> *head;
  ConcurrentRangeLock();
  bool tryLock(T start, T range_size);
  bool releaseLock(T start, T range_size);
  size_t size();
};

template <typename T, unsigned maxLevel>
ConcurrentRangeLock<T, maxLevel>::ConcurrentRangeLock() {
  auto min = std::numeric_limits<T>::min();
  auto max = std::numeric_limits<T>::max();
  head     = new Node<T>();
  tail     = new Node<T>();
  tail->initialize(max, max, maxLevel);
  head->initializeHead(min, min, maxLevel, tail);
  srand(0);
}

template <typename T, unsigned maxLevel>
size_t ConcurrentRangeLock<T, maxLevel>::size() {
  return elementsCount.load();
}

template <typename T, unsigned maxLevel>
int ConcurrentRangeLock<T, maxLevel>::randomLevel() {
  auto level = 0U;
  while (rand() % 2 && level < maxLevel) { level++; }
  return level;
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::findInsert(T start, T end, Node<T> **preds, Node<T> **succs) {
  bool marked[1] = {false};
  bool snip;
  Node<T> *pred;
  Node<T> *curr = nullptr;
  Node<T> *succ;
retry:
  while (true) {
    pred = head;
    for (int level = maxLevel; level >= 0; level--) {
      curr = pred->next[level]->getReference();
      while (start > curr->getStart()) {
        succ = curr->next[level]->get(marked);
        while (marked[0]) {
          snip = pred->next[level]->compareAndSet(curr, succ, false, false);
          if (!snip) { goto retry; }
          curr = pred->next[level]->getReference();
          succ = curr->next[level]->get(marked);
        }
        if (start >= curr->getStart()) {
          pred = curr;
          curr = succ;
        } else {
          break;
        }
      }
      preds[level] = pred;
      succs[level] = curr;
    }
    return (!(start > pred->getEnd() && end < curr->getStart()));
  }
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::findExact(T start, T end, Node<T> **preds, Node<T> **succs) {
  bool marked[1] = {false};
  bool snip;
  Node<T> *pred;
  Node<T> *curr = nullptr;
  Node<T> *succ;
retry:
  while (true) {
    pred = head;
    for (int level = maxLevel; level >= 0; level--) {
      curr = pred->next[level]->getReference();
      while (start >= curr->getStart()) {
        succ = curr->next[level]->get(marked);
        while (marked[0]) {
          snip = pred->next[level]->compareAndSet(curr, succ, false, false);
          if (!snip) { goto retry; }
          curr = pred->next[level]->getReference();
          succ = curr->next[level]->get(marked);
        }
        if (start >= curr->getEnd()) {
          pred = curr;
          curr = succ;
        } else {
          break;
        }
      }
      preds[level] = pred;
      succs[level] = curr;
    }
    return (start == curr->getStart() && end == curr->getEnd());
  }
}

template <typename T, unsigned maxLevel>
void ConcurrentRangeLock<T, maxLevel>::findDelete(T start) {
  bool marked[1] = {false};
  bool snip;
  Node<T> *pred;
  Node<T> *curr = nullptr;
  Node<T> *succ;
retry:
  while (true) {
    pred = head;
    for (int level = maxLevel; level >= 0; level--) {
      curr = pred->next[level]->getReference();
      while (start >= curr->getStart()) {
        succ = curr->next[level]->get(marked);
        while (marked[0]) {
          snip = pred->next[level]->compareAndSet(curr, succ, false, false);
          if (!snip) { goto retry; }
          curr = pred->next[level]->getReference();
          succ = curr->next[level]->get(marked);
        }
        if (start >= curr->getEnd()) {
          pred = curr;
          curr = succ;
        } else {
          break;
        }
      }
    }
    return;
  }
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::tryLock(T start, T range_size) {
  auto end     = start + range_size - 1;
  int topLevel = randomLevel();
  Node<T> *preds[maxLevel + 1];
  Node<T> *succs[maxLevel + 1];

  while (true) {
    bool found = findInsert(start, end, preds, succs);
    if (found) { return false; }
    auto newNode = new Node<T>();
    newNode->initialize(start, end, topLevel);
    for (int level = 0; level <= topLevel; ++level) {
      Node<T> *succ = succs[level];
      newNode->next[level]->store(succ, false);
    }
    auto pred = preds[0];
    auto succ = succs[0];
    newNode->next[0]->store(succ, false);
    if (!pred->next[0]->compareAndSet(succ, newNode, false, false)) { continue; }
    for (int level = 1; level <= topLevel; ++level) {
      while (true) {
        pred = preds[level];
        succ = succs[level];
        if (pred->next[level]->compareAndSet(succ, newNode, false, false)) {
          break;
        } else {
          findInsert(start, end, preds, succs);
        }
      }
    }
    elementsCount.fetch_add(1, std::memory_order_relaxed);
    return true;
  }
}

template <typename T, unsigned maxLevel>
bool ConcurrentRangeLock<T, maxLevel>::releaseLock(T start, T range_size) {
  auto end = start + range_size - 1;
  Node<T> *preds[maxLevel + 1];
  Node<T> *succs[maxLevel + 1];
  Node<T> *succ;

  while (true) {
    bool found = findExact(start, end, preds, succs);
    if (!found) {
      return false;
    }
    Node<T> *nodeToRemove = succs[0];
    for (int level = nodeToRemove->getTopLevel(); level >= 0 + 1; level--) {
      bool marked[1] = {false};
      succ           = nodeToRemove->next[level]->get(marked);
      while (!marked[0]) {
        nodeToRemove->next[level]->attemptMark(succ, true);
        succ = nodeToRemove->next[level]->get(marked);
      }
    }
    bool marked[1] = {false};
    succ           = nodeToRemove->next[0]->get(marked);
    while (true) {
      bool iMarkedIt = nodeToRemove->next[0]->compareAndSet(succ, succ, false, true);
      succ           = succs[0]->next[0]->get(marked);
      if (iMarkedIt) {
        findDelete(start);
        elementsCount.fetch_sub(1, std::memory_order_relaxed);
        return true;
      } else if (marked[0]) {
        return false;
      }
    }
  }
}
