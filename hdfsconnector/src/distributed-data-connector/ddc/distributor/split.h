/*
(c) Copyright 2015 Hewlett Packard Enterprise Development LP

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/



#ifndef DDC_DISTRIBUTOR_SPLIT_H
#define DDC_DISTRIBUTOR_SPLIT_H

#include <map>
#include <string>

#include "distributor.pb.h"

namespace ddc {
namespace distributor {

class Status{
public:
    enum value {
        PENDING,
        OK,
        ERROR
    };
};



struct SplitTrackingInfo {
    SplitTrackingInfo() :
        status(Status::PENDING),
        worker(""),
        id(""),
        filename(""),
        start(0),
        end(0)
    {

    }

    SplitTrackingInfo(const std::string& _id,
                      const std::string& _worker) :
        status(Status::PENDING),
        worker(_worker),
        id(_id),
        filename(""),
        start(0),
        end(0)
    {

    }

    Status::value status;
    std::string worker;
    std::string id;
    std::string filename;
    uint64_t start;
    uint64_t end;

    std::string schema;
    std::string objectType;
};

struct FullRequest {
    AnyRequest protoMessage;
    std::string worker; // worker this req is addressed to

};

typedef std::map<std::string, SplitTrackingInfo> SplitMap;
typedef std::map<std::string, SplitTrackingInfo>::const_iterator SplitMapConstIt;
typedef std::map<std::string, SplitTrackingInfo>::iterator SplitMapIt;



} // namespace distributor
} // namespace ddc

#endif // DDC_DISTRIBUTOR_SPLIT_H
