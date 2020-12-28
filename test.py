import asyncio


data_set = dict()


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


def process_data(data):
    message = data.rstrip().split(' ')
    answer = str()
    print('received %r' % message)
    if message[0] == 'put':
        try:
            name = message[1]
            value = float(message[2])
            timestamp = int(message[3])
            if len(message) == 4:
                if name not in data_set.keys():
                    data_set[name] = [(timestamp, value)]
                else:
                    for i in range(0, len(data_set[name])):
                        if timestamp == data_set[name][i][0]:
                            data_set[name].pop(i)
                            break
                    data_set[name].append((timestamp, value))

                answer = 'ok\n\n'
            else:
                answer = 'error\nwrong command\n\n'
        except IndexError:
            answer = 'error\nwrong command\n\n'
        except ValueError:
            answer = 'error\nwrong command\n\n'
    elif message[0] == 'get':
        try:
            name = message[1]
            if len(message) == 2:
                answer = 'ok\n'
                if name == '*':
                    for key in data_set:
                        for d in data_set[key]:
                            answer += key + ' ' + str(d[1]) + ' ' + str(d[0]) + '\n'
                else:
                    try:
                        for d in data_set[name]:
                            answer += name + ' ' + str(d[1]) + ' ' + str(d[0]) + '\n'
                    except KeyError:
                        pass
                answer += '\n'
            else:
                answer = 'error\nwrong command\n\n'
        except ValueError:
            answer = 'error\nwrong command\n\n'
        except IndexError:
            answer = 'error\nwrong command\n\n'
    else:
        answer = 'error\nwrong command\n\n'
    print('sending:\n' + answer)
    print('data_set = ' + str(data_set))
    return answer


class ClientServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = process_data(data.decode())
        self.transport.write(resp.encode())
