select date, hour, road, cam, count(sub.num) as traffic, avg(sub.speed) as avg_speed
from dominic.hive_traffic
lateral view explode(car)  subView as sub 
group by date, hour, road, cam
sort by date, hour, road, cam;