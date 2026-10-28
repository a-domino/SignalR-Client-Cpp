//
// Created by austin on 10/26/26.
//

#ifndef MICROSOFT_SIGNALR_CANCEL_STREAM_H
#define MICROSOFT_SIGNALR_CANCEL_STREAM_H


namespace signalr
{
    class cancel_stream_interface {
    public:
        virtual bool cancel() = 0;
        virtual ~cancel_stream_interface() = default;
    };
}


#endif //MICROSOFT_SIGNALR_CANCEL_STREAM_H
