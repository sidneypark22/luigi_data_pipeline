import random
import datetime

weblog_file_name = 'access.log'

ip_address_dict_list = {
    'US': ['216.244.66.239', '216.244.66.1', '216.244.1.1', '216.244.66.10', '216.244.66.12', '216.244.66.13', '216.244.66.14'],
    'CA': ['192.206.151.131', '192.206.151.1', '192.206.151.50', '192.206.151.51'],
    'AU': ['123.3.223.22', '123.3.1.22', '123.3.100.12', '123.3.100.13']
}

username_dict_list = {
    'US': ['user_1', 'user_6', 'user_7', 'user_8', 'user_9', 'user_10'],
    'CA': ['user_2', 'user_5', 'user_11', 'user_12'],
    'AU': ['user_3', 'user_4', 'user_13', 'user_14']
}

user_agent_list = [
    'Mozilla/5.0 (Linux; Android 11; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.70 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 8.0.0; SM-G960F Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.84 Mobile Safari/537.36',
    #Mozilla/5.0 (Linux; Android 7.0; SM-G892A Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/60.0.3112.107 Mobile Safari/537.36
    #Mozilla/5.0 (Linux; Android 7.0; SM-G930VC Build/NRD90M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36
    #Mozilla/5.0 (Linux; Android 6.0.1; SM-G935S Build/MMB29K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36
    #Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1
    #Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/69.0.3497.105 Mobile/15E148 Safari/605.1
    'Mozilla/5.0 (iPhone; CPU iPhone OS 12_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) GSA/194.0.419363360 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (iPad; U; CPU OS 3_2_1 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Mobile/7B405',
    'Mozilla/5.0 (iPad; CPU OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60',
]

start_datetime_epoach = datetime.datetime(year=2021, month=1, day=1).timestamp()
end_datetime_epoach = datetime.datetime(year=2022, month=1, day=14).timestamp()

weblog_file = open(weblog_file_name, 'a')

for i in range(100000):
    random_country = random.choice(['US', 'CA', 'AU'])
    random_timestamp_epoach = random.randrange(start_datetime_epoach, end_datetime_epoach)
    ip_address = random.choice(ip_address_dict_list[random_country])
    identity = '-'
    username = random.choice(username_dict_list[random_country])
    timestamp = '[{}]'.format(datetime.datetime.fromtimestamp(random_timestamp_epoach).strftime('%d/%h/%Y:%H:%M:%S +0000'))
    get_request = '"GET /place-order HTTP/1.1"'
    server_status_code = '200'
    response_size = '50'
    referer = '"-"'
    user_agent = '"{}"'.format(random.choice(user_agent_list))
    log_line = '{ip_address} {identity} {username} {timestamp} {get_request} {server_status_code} {response_size} {referer} {user_agent}'.format(
        ip_address=ip_address,
        identity=identity,
        username=username,
        timestamp=timestamp,
        get_request=get_request,
        server_status_code=server_status_code,
        response_size=response_size,
        referer=referer,
        user_agent=user_agent
    )
    weblog_file.write(log_line + '\n')

weblog_file.close()

