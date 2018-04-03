INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020101_020201' SELECT md5(deviceid)
  FROM dmp.tag_dev WHERE ds_last BETWEEN '2017-07-01' AND '2018-03-29' AND app_cat = '020101' AND tid IN ('020201');

INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020709or020708_020201or020202'
  SELECT md5(deviceid) FROM dmp.tag_dev WHERE ds_last BETWEEN '2017-07-01' AND '2018-03-29' AND app_cat IN ('020708','020709') AND tid IN ('020201','020202');

INSERT OVERWRITE LOCAL DIRECTORY '/home/ec2-user/dmp/wangjf/data/020627_020201or020202'
  SELECT md5(deviceid) FROM dmp.tag_dev WHERE ds_last BETWEEN '2017-07-01' AND '2018-03-29' AND app_cat IN ('020627') AND tid IN ('020201','020202');