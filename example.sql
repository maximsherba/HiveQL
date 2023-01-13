--Выбрать любой интересный открытый набор данных для работы (можно воспользоваться сайтом Kaggle, пример набора данных: https://www.kaggle.com/tylerx/flights-and-airports-data)
--Создать database в Hive и загрузить туда эти таблицы
--На этих данных построить витрины (5-6) с использованием конструкций: where, count, group by, having, order by, join, union, window.
--К каждой созданной витрине добавить описание, что данная витрина демонстрирует (например: количество перелетов за год, топ самых загруженных аэропортов и т.п.).

--docker cp airports.csv <datanode>:/hive-data/airports.csv
--root@c2a188ef02d9:/# hdfs dfs -mkdir /hive-data
--root@c2a188ef02d9:/# hdfs dfs -copyFromLocal /hive-data/* /hive-data/

create database hivetest;

use hivetest;

drop table if exists hivetest.airports;

CREATE TABLE hivetest.airports (
		airport_id int,
		city string,
		state string,
		name string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/hive-data/airports.csv' OVERWRITE
INTO
	TABLE hivetest.airports;

select
	*
from
	hivetest.airports limit 10;
--
drop table if exists hivetest.flights;
CREATE TABLE hivetest.flights (
		DayofMonth int,
		DayOfWeek int,
		Carrier string,
		OriginAirportID int,
		DestAirportID int,
		DepDelay int,
		ArrDelay int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/hive-data/flights.csv' OVERWRITE
INTO
	TABLE hivetest.flights;

select
	*
from
	hivetest.flights limit 10;
--
drop table if exists hivetest.raw_flight_data;
CREATE TABLE hivetest.raw_flight_data (
		DayofMonth int,
		DayOfWeek int,
		Carrier string,
		OriginAirportID int,
		DestAirportID int,
		DepDelay int,
		ArrDelay int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/hive-data/raw-flight-data.csv' OVERWRITE
INTO
	TABLE hivetest.raw_flight_data;

select
	*
from
	hivetest.raw_flight_data limit 10;
--
drop table if exists hivetest.flight_reconciliation;
create table hivetest.flight_reconciliation as
select
	coalesce(rf.dayofmonth, f.dayofmonth) as dayofmonth,
	coalesce(rf.dayofweek, f.dayofweek) as dayofweek,
	coalesce(rf.carrier, f.carrier) as carrier,
	coalesce(rf.originairportid, f.originairportid) as originairportid,
	coalesce(rf.destairportid, f.destairportid) as destairportid,
	case
		when rf.carrier is NULL then 'RIGHT'
		when f.carrier is NULL then 'LEFT'
		else 'BOTH'
	end as where_exists,
	rf.DepDelay as rf_DepDelay,
	rf.ArrDelay as rf_ArrDelay,
	f.DepDelay as f_DepDelay,
	f.ArrDelay as f_ArrDelay
from
	hivetest.raw_flight_data rf
full outer join hivetest.flights f
on
	rf.dayofmonth = f.dayofmonth
	and rf.carrier = f.carrier
	and rf.originairportid = f.originairportid
	and rf.destairportid = f.destairportid;

ALTER TABLE hivetest.dayofweek_analysis SET
TBLPROPERTIES ('comment' = 'hivetest.flight_reconciliation');
--
drop table if exists hivetest.delays_by_dayofmonth;
create table hivetest.delays_by_dayofmonth as
select
	dayofmonth,
	sum(rf_DepDelay) as rf_DepDelay,
	sum(f_DepDelay) as f_DepDelay
from
	hivetest.flight_reconciliation
where
	where_exists = 'BOTH'
group by
	dayofmonth
HAVING
	sum(rf_DepDelay) > 10
order by
	dayofmonth;

ALTER TABLE hivetest.dayofweek_analysis SET
TBLPROPERTIES ('comment' = 'hivetest.delays_by_dayofmonth');
--
drop table if exists hivetest.carrier_delay_by_source;
create table hivetest.carrier_delay_by_source as
select
	carrier,
	originairportid,
	destairportid,
	count(*) as cnt,
	sum(DepDelay) as sum_DepDelay,
	sum(ArrDelay) as sum_ArrDelay,
	'flights' as source
from
	hivetest.flights
group by
	carrier,
	originairportid,
	destairportid
union all
select
	carrier,
	originairportid,
	destairportid,
	count(*) as cnt,
	sum(DepDelay) as sum_DepDelay,
	sum(ArrDelay) as sum_ArrDelay,
	'raw_flight_data' as source
from
	hivetest.raw_flight_data
group by
	carrier,
	originairportid,
	destairportid;

ALTER TABLE hivetest.dayofweek_analysis SET
TBLPROPERTIES ('comment' = 'hivetest.raw_flight_data');
--
drop table if exists hivetest.dayofweek_analysis;
create table hivetest.dayofweek_analysis as 
select
	dayofweek,
	state,
	depdelay,
	lag(depdelay) over(PARTITION by state
order by
	dayofweek) as lag_depdelay,
	depdelay - lag(depdelay) over(
	order by dayofweek) as diff
from
	(
	select
		dayofweek,
		originairportid,
		sum(depdelay) as depdelay
	from
		hivetest.flights
	group by
		dayofweek,
		originairportid
) f
join hivetest.airports a 
on
	f.originairportid = a.airport_id ;

ALTER TABLE hivetest.dayofweek_analysis SET
TBLPROPERTIES ('comment' = 'hivetest.dayofweek_analysis');
--
drop table if exists hivetest.city_by_dayofmonth;
create table hivetest.city_by_dayofmonth as
select
	dayofmonth,
	city,
	depdelay,
	sum(depdelay) over(PARTITION by city
order by
	dayofmonth) rolsum_depdelay
from
	(
	select
		dayofmonth,
		city,
		sum(depdelay) as depdelay
	from
		hivetest.flights f
	left join hivetest.airports a
on
		a.airport_id = f.destairportid
		and carrier = 'DL'
	group by
		dayofmonth,
		city) t;

ALTER TABLE hivetest.dayofweek_analysis SET
TBLPROPERTIES ('comment' = 'hivetest.city_by_dayofmonth');