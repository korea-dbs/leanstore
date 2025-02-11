#pragma once

#include <limits>
#include <memory>
#include <vector>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <numeric>
#include <functional>
#include <cmath>
#include <mutex>

template <typename storageType, typename valueType>
class Hist
{
public:
	int size;
	valueType from = std::numeric_limits<valueType>::max();
	valueType to = std::numeric_limits<valueType>::min();
	std::mutex lock;
	
	std::vector<storageType> histData;
	valueType min = std::numeric_limits<valueType>::max();
	valueType max = std::numeric_limits<valueType>::min();
	long counter = 0;
	double total = 0;

	Hist(storageType size = 1, valueType from = 0, valueType to = 1)  : size(size), from(from), to(to) {
      std::unique_lock<std::mutex> ul(lock);
      histData.resize(size);
   }

	Hist(const Hist&) = delete;
	Hist& operator=(Hist&) = delete;
	
	int increaseSlot(valueType value) {
		counter++;
		min = std::min(min, value);
		max = std::max(max, value);
		total += value;
		if (value < from)
			value = from;
		if (value >= to)
			value = to-1;
		const valueType normalizedValue = (value - from) /(double) (to - from) * size;
		histData.at(normalizedValue)++;
		return normalizedValue;
	}
	
	void print() {
		//std::cout << "[";
		//std::cout << "(size:" << size << ",from:" << from << ",to:" << to << "),";
		std::copy(histData.begin(),
				  histData.end(),
				  std::ostream_iterator<storageType>(std::cout,",")
				  );
		//std::cout << "]";
	}
	
	valueType getPercentile(float iThPercentile) {
      std::unique_lock<std::mutex> ul(lock);
		long sum = std::accumulate(histData.begin(), histData.end(), 0, std::plus<int>());
		long sumUntilPercentile = 0;
		const long percentile = sum * (iThPercentile / 100.0);
		valueType i = 0;
		for (; sumUntilPercentile < percentile; i++) {
			sumUntilPercentile += histData[i];
		}
		return from + (i /(float) size * (to - from));
	}

	void writePercentiles(std::ostream& out) {
		out << getPercentile(50) << ","
			<< getPercentile(90) << ","
			<< getPercentile(95) << ","
			<< getPercentile(99) << ","
			<< getPercentile(99.5) << ","
			<< getPercentile(99.9) << ","
			<< getPercentile(99.99); 
	} 

	Hist& operator+=(const Hist<storageType, valueType> &rhs) {
		// only allow aggregation of hists with the same size and range
		// except if the left hand sight has default values, then set it up
		if (this->size == 1 && this->from == 0 && this->to == 1) {
			this->size = rhs.size;
			this->from = rhs.from;
			this->to = rhs.to;
			this->histData.resize(this->size);
		}
		if (size != rhs.size || from != rhs.from || to != rhs.to ) {
			throw std::logic_error("Cannot add two different hists.");
		}
		if (rhs.counter > 0) {
			for (int i = 0; i < size; i++) {
				this->histData[i] += rhs.histData[i];
			}
			this->counter += rhs.counter;
			this->total += rhs.total;
			this->min = std::min(this->min, rhs.min);
			this->max = std::max(this->max, rhs.max);
		}
		return *this;
	}
	
	Hist& operator+(const Hist<storageType, valueType> &rhs) {
		auto result = *this;
		return result += rhs;
	}
	valueType getMin() {
		return min;
	}
	valueType getMax() {
		return max;
	}
	double getAvg() {
		return (double)total / counter;
	}
	valueType getCounter() {
		return counter;
	}
	
	void resetData() {
		counter = 0;
		total = 0;
		min = std::numeric_limits<valueType>::max();
		max = std::numeric_limits<valueType>::min();
		std::fill(histData.begin(), histData.end(), 0);
	}

private:
};