INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020101_020201' SELECT md5(deviceid)
  FROM dmp.tag_dev WHERE ds_last BETWEEN '2017-07-01' AND '2018-03-29' AND app_cat = '020101' AND tid IN ('020201');

INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020709or020708_020201or020202'
  SELECT md5(deviceid) FROM dmp.tag_dev WHERE ds_last BETWEEN '2017-07-01' AND '2018-03-29' AND app_cat IN ('020708','020709') AND tid IN ('020201','020202');

INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020627_020201or020202'
  SELECT md5(deviceid) FROM dmp.tag_dev WHERE ds_last BETWEEN '2017-07-01' AND '2018-03-29' AND app_cat IN ('020627') AND tid IN ('020201','020202');

-- 包1：imei，MD5，设备号，二次元风格
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020705'
  SELECT DISTINCT md5(devid) FROM dmp.tag_dev_game WHERE app_cat = '020705' AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包2：imei，MD5，设备号，日系风格
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020714'
  SELECT DISTINCT md5(devid) FROM dmp.tag_dev_game WHERE app_cat = '020714' AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包3：imei，MD5，设备号，角色扮演
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020101_020301or020302'
  SELECT DISTINCT md5(devid) FROM dmp.tag_dev_game WHERE app_cat = '020101' AND tid IN ('020301','020302') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包4：imei，MD5，设备号，二次元风格or日系风格or角色扮演的中、高频
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020705or020714or020101_020301or020302'
   SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020705','020714','020101') AND tid IN ('020301','020302') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包5：imei，MD5，设备号，二次元风格or日系风格or角色扮演的中、高粘性
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020705or020714or020101_020401or020402'
   SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020705','020714','020101') AND tid IN ('020401','020402') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包6：imei，MD5，设备号，二次元风格or日系风格or角色扮演的中、高付费
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020705or020714or020101_020201or020202'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020705','020714','020101') AND tid IN ('020201','020202') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包1：imei，MD5，设备号，宫廷or复古or古风or模拟经营的中、高粘性
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020627or020708or020709or020106_020401or020402'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020627','020708','020709','020106') AND tid IN ('020401','020402') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

  SELECT COUNT(DISTINCT md5(devid)) counts FROM dmp.tag_dev_game WHERE app_cat IN ('020627','020708','020709','020106') AND tid IN ('020401','020402') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包2：imei，MD5，设备号，宫廷or复古or古风or模拟经营的中、高频次
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020627or020708or020709or020106_020301or020302'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020627','020708','020709','020106') AND tid IN ('020301','020302') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 包3：imei，MD5，设备号，宫廷or复古or古风or模拟经营的中、高付费
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020627or020708or020709or020106_020201or020202'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020627','020708','020709','020106') AND tid IN ('020201','020202') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

SET mapred.reduce.tasks = 1;
-- 包1：idfa，MD5，设备号，宫廷or复古or古风or模拟经营的中、高粘性
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/idfa/020627or020708or020709or020106_020401or020402'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020627','020708','020709','020106') AND tid IN ('020401','020402') AND ctype = 'idfa' AND devid IS NOT NULL AND devid <> '';

-- 包2：idfa，MD5，设备号，宫廷or复古or古风or模拟经营的中、高频次
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/idfa/020627or020708or020709or020106_020301or020302'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020627','020708','020709','020106') AND tid IN ('020301','020302') AND ctype = 'idfa' AND devid IS NOT NULL AND devid <> '';

-- 包3：idfa，MD5，设备号，宫廷or复古or古风or模拟经营的中、高付费
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/idfa/020627or020708or020709or020106_020201or020202'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020627','020708','020709','020106') AND tid IN ('020201','020202') AND ctype = 'idfa' AND devid IS NOT NULL AND devid <> '';

CREATE TABLE apps(
    app_name STRING,
    app_type STRING,
    id_type STRING
)ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

SET mapred.reduce.task=1;
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-09/register_idfa'
 SELECT DISTINCT tkio.device_id FROM (SELECT md5(xcontext['_idfa']) device_id,appkey app_key FROM dmp.event_tkio WHERE xwhere = 'register'
 AND ds BETWEEN '2017-06-01' AND '2018-04-01' AND xcontext['_idfa'] IS NOT NULL AND xcontext['_idfa'] <> '' AND xcontext['_idfa'] <> '00000000-0000-0000-0000-000000000000') tkio INNER JOIN
 (SELECT DISTINCT info.app_key app_key FROM dmp.app_info info,dmp.apps app WHERE info.app_name LIKE concat('%',app.app_name,'%')) app ON tkio.app_key = app.app_key;

INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-09/register_imei'
 SELECT DISTINCT tkio.device_id FROM (SELECT md5(xcontext['_imei']) device_id,appkey app_key FROM dmp.event_tkio WHERE xwhere = 'register'
 AND ds BETWEEN '2017-06-01' AND '2018-04-01' AND xcontext['_imei'] IS NOT NULL AND xcontext['_imei'] <> '') tkio INNER JOIN
 (SELECT DISTINCT info.app_key app_key FROM dmp.app_info info,dmp.apps app WHERE info.app_name LIKE concat('%',app.app_name,'%')) app ON tkio.app_key = app.app_key;

-- 付费idfa
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-09/payment_idfa'
 SELECT DISTINCT tkio.device_id FROM (SELECT device_id,app_key FROM (SELECT md5(xcontext['_idfa']) device_id,appkey app_key,SUM(xcontext['_currencyamount']) amount
 FROM dmp.event_tkio WHERE ds BETWEEN '2017-06-01' AND '2018-04-01' AND xwhere = 'payment' AND xcontext['_currencyamount'] LIKE '%.%' AND xcontext['_idfa'] IS NOT NULL
 AND xcontext['_idfa'] <> '' AND xcontext['_idfa'] <> '00000000-0000-0000-0000-000000000000' GROUP BY xcontext['_idfa'],appkey) payment WHERE amount > 100) tkio INNER JOIN (SELECT DISTINCT info.app_key app_key FROM dmp.app_info info,
 dmp.apps app WHERE info.app_name LIKE concat('%',app.app_name,'%')) app ON tkio.app_key = app.app_key;

-- 付费imei
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-09/payment_imei'
 SELECT DISTINCT tkio.device_id FROM (SELECT device_id,app_key FROM (SELECT md5(xcontext['_imei']) device_id,appkey app_key,SUM(xcontext['_currencyamount']) amount
 FROM dmp.event_tkio WHERE ds BETWEEN '2017-06-01' AND '2018-04-01' AND xwhere = 'payment' AND xcontext['_currencyamount'] LIKE '%.%' AND xcontext['_imei'] IS NOT NULL
 AND xcontext['_imei'] <> '' GROUP BY xcontext['_imei'],appkey) payment WHERE amount > 100) tkio INNER JOIN (SELECT DISTINCT info.app_key app_key FROM dmp.app_info info,
 dmp.apps app WHERE info.app_name LIKE concat('%',app.app_name,'%')) app ON tkio.app_key = app.app_key;


SELECT xcontext['_payment_type'] FROM dmp.event_tkio WHERE ds = '2018-03-01' AND xwhere = 'payment' GROUP BY xcontext['_payment_type'];

-- 注册包idfa
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-09/注册包idfa'
SELECT DISTINCT xcontext['_idfa'] FROM dmp.event_tkio WHERE ds BETWEEN '2017-10-01' AND '2018-04-01' AND appkey IN
  (SELECT DISTINCT info.app_key FROM dmp.app_info info,dmp.apps app WHERE info.app_name LIKE concat('%',app.app_name,'%'));

-- 注册包imei
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-09/注册包imei'
SELECT DISTINCT xcontext['_imei'] FROM dmp.event_tkio WHERE ds BETWEEN '2017-10-01' AND '2018-04-01' AND appkey IN
  (SELECT DISTINCT info.app_key FROM dmp.app_info info,dmp.apps app WHERE info.app_name LIKE concat('%',app.app_name,'%'));

SET mapred.reduce.tasks = 1;
-- 放置游戏
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-11/包1'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat = '020120' AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 战争题材
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-11/包2'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat = '020615' AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- 射击类型
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-11/包3'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat = '020103' AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- （模拟经营or放置游戏or战争题材or射击）的中、高付费
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-11/包4'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020106','020120','020615','020103') AND tid IN ('020201','020202') AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

-- （模拟经营or放置游戏or战争题材or射击）的高粘性
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/2018-04-11/包5'
  SELECT DISTINCT md5(devid) device_id FROM dmp.tag_dev_game WHERE app_cat IN ('020106','020120','020615','020103') AND tid = '020401' AND ctype = 'imei' AND devid IS NOT NULL AND devid <> '';

SET mapred.reduce.tasks = 1;
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/top_app'
  SELECT appkey,COUNT(1) counts FROM dmp.event_tkio WHERE ds BETWEEN '2018-03-01' AND '2018-04-01' GROUP BY appkey ORDER BY counts;

DROP TABLE dmp_dev.tag_dev_game;
CREATE TABLE dmp_dev.tag_dev_game(
 device_id STRING,
 app_cat STRING,
 tid STRING,
 ds STRING,
 ctype STRING
)row format delimited
fields terminated by ','
LOCATION 's3://reyunbpu/dmp_dev/tag_dev_game';

SET mapred.reduce.tasks = 1;
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/tag_dev_game/imei'
  SELECT devid device_id,app_cat,tid,dt_tag ds,ctype FROM dmp.tag_dev_game WHERE dt_tag = '2018-04-04' AND ctype = 'imei' LIMIT 100000;

SET mapred.reduce.tasks = 1;
INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/tag_dev_game/idfa'
  SELECT devid device_id,app_cat,tid,dt_tag ds,ctype FROM dmp.tag_dev_game WHERE dt_tag = '2018-04-04' AND ctype = 'idfa' LIMIT 100000;

DROP TABLE dmp.close_effect;
CREATE TABLE dmp.close_effect(
  device_id STRING,
  device_type STRING,
  event_type STRING,
  app_key STRING,
  app_name STRING,
  join_date STRING,
  sdkanalysis_date STRING
)STORED AS orc
LOCATION 's3://reyunbpu/dmp/close_effect';

CREATE TABLE app_key_tmp(
  app_key STRING
)
drop table app_top;
CREATE  TABLE  app_top(
  app_key string,
  name string,
  platform string,
  create_time string
  )
row format delimited
fields terminated by ','
LOCATION's3://reyunbpu/dmp/app_top';

INSERT OVERWRITE TABLE dmp.app_top SELECT DISTINCT tkio.app_key,tkio.name,tkio.platform,tkio.create_time FROM dmp.app_tkio tkio INNER JOIN dmp.app_key_tmp tmp ON tkio.app_key = tmp.app_key;

SELECT app_key,name,platform,create_time FROM dmp.app_tkio;

SET mapred.reduce.tasks = 1;
INSERT INTO TABLE dmp.close_effect SELECT DISTINCT xcontext['${hiveconf:os_type}'] device_id,'${hiveconf:platform}' device_type,xwhere event_type,'${hiveconf:app_key}' app_key,
 '${hiveconf:app_name}' app_name,xwhen join_date,'${hiveconf:create_time}' sdkanalysis_date FROM dmp.event_tkio WHERE ds BETWEEN '2017-01-01' AND '2017-12-31' AND appkey = '${hiveconf:app_key}';