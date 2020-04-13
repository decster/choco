#include <ctype.h>
#include "slice.h"
#include "common.h"

namespace choco {

bool Slice::startsWith(const Slice& s) const {
	return s.size() == 0 || (size_ >= s.size() && memcmp(data_, s.data(), s.size()) == 0);
}

bool Slice::endsWith(const Slice& s) const {
	return s.size() == 0 || (size_ >= s.size() && memcmp(data_+size_-s.size(), s.data(), s.size() == 0));
}

//int64_t Slice::toInt(int64_t default_value=0) const {
//	return strtoll(data_, nullptr, 10);
//}
//
//double Slice::toDouble(double default_value=0) const {
//
//}

Slice Slice::trim() const {
	Slice ret(*this);
	for (size_t i=0;i<size_;i++) {
		if (!isspace(ret[i])) {
			ret.data_ += i;
			ret.size_ -= i;
			break;
		}
	}
	for (ssize_t i=ret.size_;i>0;i--) {
		if (!isspace(ret[i-1])) {
			ret.size_ = i;
			break;
		}
	}
	return ret;
}


std::vector<Slice> Slice::split(const char sep, bool trim) const {
	std::vector<Slice> ret;
	size_t last = 0;
	for (size_t i=0; i < size(); i++) {
		if (data_[i] == sep) {
			Slice tmp(data_+last, i-last);
			if (trim) {
				Slice ttmp = tmp.trim();
				if (ttmp.size() > 0) {
					ret.push_back(ttmp);
				}
			} else {
				ret.emplace_back(tmp);
			}
			last = i+1;
		}
	}
	Slice tmp(data_+last, size()-last);
	if (trim) {
		Slice ttmp = tmp.trim();
		if (ttmp.size() > 0) {
			ret.push_back(ttmp);
		}
	} else {
		ret.emplace_back(tmp);
	}
	//DLOG(INFO) << "split result: " << join(ret, ":");
	return ret;
}

string Slice::join(const std::vector<Slice>& strs, const Slice& sep) {
	string ret;
	if (strs.size() == 0) {
		return ret;
	}
	size_t sz = sep.size() * (strs.size()-1);
	for (size_t i=0;i<strs.size();i++) {
		sz += strs[i].size();
	}
	ret.reserve(sz);
	for (size_t i=0;i<strs.size();i++) {
		if (i>0) {
			ret.append((const char*)sep.data(), sep.size());
		}
		ret.append((const char*)strs[i].data(), strs[i].size());
	}
	return ret;
}

}
