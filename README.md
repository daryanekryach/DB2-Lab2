# DB2-Lab2
[![Build Status](https://travis-ci.org/daryanekryach/DB2-Lab2.svg?branch=master)](https://travis-ci.org/daryanekryach/DB2-Lab2)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b5dcceb1e9e54fc4aa319e0117a29db0)](https://www.codacy.com/app/daryanekryach/DB2-Lab2?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=daryanekryach/DB2-Lab2&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/b5dcceb1e9e54fc4aa319e0117a29db0)](https://www.codacy.com/app/daryanekryach/DB2-Lab2?utm_source=github.com&utm_medium=referral&utm_content=daryanekryach/DB2-Lab2&utm_campaign=Badge_Coverage)
[![codecov](https://codecov.io/gh/daryanekryach/DB2-Lab2/branch/master/graph/badge.svg)](https://codecov.io/gh/daryanekryach/DB2-Lab2)

*This is the repository for 2nd lab of Databases-2 with MongoDB*

***

## RESULTS
### Metrics
> Execution time is 2 min 54 sec<br/>Used memory is 5.916160583496094 mb

### Ordered list of  IP-adresses, who visited website with specific URL
>Document{{_id=5a12b4042281a22438f4dba1, URL=https://drive.google.com, IP=5.5.5.5, timeStamp=Mon Nov 06 18:35:00 EET 2017, timeSpent=12000}}<br>
>Document{{_id=5a12b4042281a22438f4dba3, URL=https://open.spotify.com, IP=5.5.5.5, timeStamp=Tue Nov 07 14:18:00 EET 2017, timeSpent=10000}}<br>
>Document{{_id=5a12b4042281a22438f4db9e, URL=https://open.spotify.com, IP=4.4.4.4, timeStamp=Wed Nov 08 14:18:00 EET 2017, timeSpent=150000}}<br>
>Document{{_id=5a12b4042281a22438f4db9d, URL=https://github.com, IP=3.3.3.3, timeStamp=Sun Nov 05 11:25:00 EET 2017, timeSpent=800}}<br>
>Document{{_id=5a12b4042281a22438f4dba2, URL=https://github.com, IP=3.3.3.3, timeStamp=Fri Nov 03 11:25:00 EET 2017, timeSpent=8900}}<br>
>Document{{_id=5a12b4042281a22438f4db9b, URL=https://github.com, IP=2.2.2.2, timeStamp=Mon Nov 06 18:10:00 EET 2017, timeSpent=67000}}<br>
>Document{{_id=5a12b4042281a22438f4dba0, URL=https://github.com, IP=2.2.2.2, timeStamp=Mon Nov 06 20:19:00 EET 2017, timeSpent=1500}}<br>
>Document{{_id=5a12b4042281a22438f4dba6, URL=https://drive.google.com, IP=2.2.2.2, timeStamp=Fri Nov 10 18:35:00 EET 2017, timeSpent=19000}}<br>
>Document{{_id=5a12b4042281a22438f4db9a, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Sun Nov 05 16:00:00 EET 2017, timeSpent=7000}}<br>
>Document{{_id=5a12b4042281a22438f4db9c, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Mon Nov 06 11:05:00 EET 2017, timeSpent=860000}}<br>
>Document{{_id=5a12b4042281a22438f4db9f, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=26000}}<br>
>Document{{_id=5a12b4042281a22438f4dba4, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=56000}}<br>
>Document{{_id=5a12b4042281a22438f4dba5, URL=https://github.com, IP=1.1.1.1, timeStamp=Mon Nov 06 20:19:00 EET 2017, timeSpent=1500}}
### Ordered list of  URLs, that were visited during specific period "2017-11-05 10:00:00 - 2017-11-06 20:00:00"
>Document{{_id=5a12b4042281a22438f4db9a, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Sun Nov 05 16:00:00 EET 2017, timeSpent=7000}}<br>
>Document{{_id=5a12b4042281a22438f4db9c, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Mon Nov 06 11:05:00 EET 2017, timeSpent=860000}}<br>
>Document{{_id=5a12b4042281a22438f4db9f, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=26000}}<br>
>Document{{_id=5a12b4042281a22438f4dba4, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=56000}}<br>
>Document{{_id=5a12b4042281a22438f4db9b, URL=https://github.com, IP=2.2.2.2, timeStamp=Mon Nov 06 18:10:00 EET 2017, timeSpent=67000}}<br>
>Document{{_id=5a12b4042281a22438f4db9d, URL=https://github.com, IP=3.3.3.3, timeStamp=Sun Nov 05 11:25:00 EET 2017, timeSpent=800}}<br>
>Document{{_id=5a12b4042281a22438f4dba1, URL=https://drive.google.com, IP=5.5.5.5, timeStamp=Mon Nov 06 18:35:00 EET 2017, timeSpent=12000}}<br>
### Ordered list of  URLs, that were visited by user with specific IP - "1.1.1.1"
>Document{{_id=5a12b4042281a22438f4db9a, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Sun Nov 05 16:00:00 EET 2017, timeSpent=7000}}<br>
>Document{{_id=5a12b4042281a22438f4db9c, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Mon Nov 06 11:05:00 EET 2017, >timeSpent=860000}}
>Document{{_id=5a12b4042281a22438f4db9f, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=26000}}<br>
>Document{{_id=5a12b4042281a22438f4dba4, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=56000}}<br>
>Document{{_id=5a12b4042281a22438f4dba5, URL=https://github.com, IP=1.1.1.1, timeStamp=Mon Nov 06 20:19:00 EET 2017, timeSpent=1500}}<br>
### Ordered list of URLs, with sum duration of visits in descending order
>Document{{_id=5a12b4042281a22438f4db9a, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Sun Nov 05 16:00:00 EET 2017, timeSpent=7000}}<br>
>Document{{_id=5a12b4042281a22438f4db9c, URL=https://tutorialspoint.com, IP=1.1.1.1, timeStamp=Mon Nov 06 11:05:00 EET 2017, timeSpent=860000}}<br>
>Document{{_id=5a12b4042281a22438f4db9f, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=26000}}<br>
>Document{{_id=5a12b4042281a22438f4dba4, URL=https://stackoverflow.com, IP=1.1.1.1, timeStamp=Mon Nov 06 10:02:00 EET 2017, timeSpent=56000}}<br>
>Document{{_id=5a12b4042281a22438f4dba5, URL=https://github.com, IP=1.1.1.1, timeStamp=Mon Nov 06 20:19:00 EET 2017, timeSpent=1500}}
### Ordered list of URLs, with sum amount of visits in descending order
>Document{{_id=https://drive.google.com, value=31000.0}}<br>
>Document{{_id=https://github.com, value=79700.0}}<br>
>Document{{_id=https://open.spotify.com, value=160000.0}}<br>
>Document{{_id=https://stackoverflow.com, value=82000.0}}<br>
>Document{{_id=https://tutorialspoint.com, value=867000.0}}<br>
### Ordered list of URLs, with amount of visits of each per day in specific period in descending order of visits
>Document{{_id=https://github.com, value=5.0}}<br>
>Document{{_id=https://drive.google.com, value=2.0}}<br>
>Document{{_id=https://open.spotify.com, value=2.0}}<br>
>Document{{_id=https://stackoverflow.com, value=2.0}}<br>
>Document{{_id=https://tutorialspoint.com, value=2.0}}<br>
### Ordered list of IP-adresses, with sum amount and duration of visits ordered by adress, in descending order of amount and duration of visits
>Document{{_id=2.2.2.2, value=Document{{count=3.0, duration=87500.0}}}}<br>
>Document{{_id=3.3.3.3, value=Document{{count=2.0, duration=9700.0}}}}<br>
>Document{{_id=4.4.4.4, value=Document{{count=1.0, duration=150000}}}}<br>
>Document{{_id=5.5.5.5, value=Document{{count=2.0, duration=22000.0}}}}<br>
