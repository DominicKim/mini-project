select date, hour, road, cam_num, count(sub.car_num) as traffic, avg(sub.car_speed) as avg_speed
from dominic.tbl_traffic
lateral view explode(car_info)  subView as sub 
group by date, hour, road, cam_num
sort by date, hour, road, cam_num;