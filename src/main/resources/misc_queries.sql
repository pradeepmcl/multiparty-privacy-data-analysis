# Merging data from multiple sources

insert into multiparty_privacy_3.turker_picturesurvey_response (mturk_id, response_time, scenario_bundle_id, scenario_id, image_sensitivity, image_sentiment, image_relationship, image_people_count, case1_policy, case1_policy_other, case1_policy_justification, case2_policy, case2_policy_other, case2_policy_justification, case3_policy, case3_policy_other, case3_policy_justification)
select mturk_id, response_time, scenario_bundle_id, scenario_id, image_sensitivity, image_sentiment, image_relationship, image_people_count, case1_policy, case1_policy_other, case1_policy_justification, case2_policy, case2_policy_other, case2_policy_justification, case3_policy, case3_policy_other, case3_policy_justification from multiparty_privacy_1.turker_picturesurvey_response 
where multiparty_privacy_1.turker_picturesurvey_response.scenario_id not in (1101,1102,3,1103,7,1099,1093,13,19,1119,21,23,1115,22,25,24,1111,26,1105,1104,31,1106,1134,35,1135,1133,39,1131,1125,45,51,1149,55,54,53,1143,58,57,56,63,1138,1137,1136,69,75,85,84,83,90,102,96,110,111,119,1083,117,1087,112,127,126,125,124,123,122,121,120,137,1221,136,1220,139,1223,138,1222,141,1217,140,1216,143,1219,142,1218,129,128,131,130,1230,133,1225,132,1224,135,134,1236,156,144,145,150,1242,171,1255,170,1254,1253,168,1252,175,1251,174,1250,173,172,1248,1263,162,1260,1267,1279,180,1273,1155,1163,1165,1164,1170,1176,1182,1191,1190,1192,1199,1197,1202,1203,1200,1201,1206,1207,1204,1205,1210,1211,1208,1209,1214,1215,1212,1213,1372,1371,1370,1362,1356,1350,1344,1345,1405,1404,1401,1400,1403,1402,1397,1396,1399,1398,1393,1392,1395,1394,1388,1389,1390,1391,1384,1385,1386,1387,1380,1381,1382,1383,1377,1379,1305,1311,1299,1291,1295,1282,1283,1281,1286,1284,1285,373,1343,381,1329,383,382,379,1335,1323,1313,1314,367,1315,1316,1317,1318,363,1494,1495,411,1493,414,1491,415,413,1503,1498,1496,1497,405,1479,395,1475,399,386,1485,385,384,391,1524,1525,443,444,445,1523,435,1530,1509,429,417,416,418,1515,423,479,477,1430,1431,1428,472,1434,470,1435,471,1432,1433,1410,462,456,1416,1422,450,510,1459,504,1461,505,1462,1463,1464,500,1465,501,1466,502,503,496,497,498,1471,499,493,1440,492,1443,495,494,489,488,1447,491,490,485,484,487,486,1453,481,480,483,482,547,559,553,566,565,564,563,562,561,575,571,1608,516,1610,1611,1612,1613,1614,1615,1602,522,533,532,535,534,528,531,530,540,543,1620,1582,1583,1580,1581,609,1578,1579,615,1576,1577,1574,1575,1572,1573,1570,1571,623,1568,1569,625,1596,624,630,1590,1585,1584,636,1550,1551,579,585,1542,1536,591,1565,593,1564,1567,595,1566,594,1561,597,1560,596,1563,1562,598,1557,1559,603,1552,685,684,683,682,681,680,679,678,677,676,675,674,673,672,702,696,690,652,651,650,642,668,669,670,671,664,665,666,667,660,661,662,663,657,659,746,745,744,751,739,743,742,741,765,755,759,713,712,715,714,708,711,710,733,720,723,727,1912,822,1917,816,1919,831,830,1911,1910,1896,804,805,1902,803,1890,810,1883,789,1885,1884,1875,795,774,775,773,771,1869,1858,783,1856,1857,778,1863,776,777,1853,1855,1854,882,1851,1845,888,891,890,893,892,895,894,864,865,1839,870,1835,1831,1824,876,1825,1826,1823,851,1822,850,1821,849,848,1819,855,854,853,852,859,858,1813,857,856,863,862,861,860,1807,832,1803,839,837,842,843,840,841,846,847,844,845,900,1972,1973,1974,1975,1968,1970,1971,1980,1956,1962,1942,1943,1940,1941,1938,1939,1936,1937,1950,1944,1945,1927,1926,1925,1924,1923,1922,1921,1920,1935,1934,1933,1932,1931,1930,1929,1928);

insert into multiparty_privacy_3.turker_presurvey_response (mturk_id, response_time, gender, age, education, socialmedia_frequency, sharing_frequency)
select mturk_id, response_time, gender, age, education, socialmedia_frequency, sharing_frequency from multiparty_privacy_1.turker_presurvey_response;

insert into multiparty_privacy_3.turker_postsurvey_response (mturk_id, response_time, scenario_bundle_id, sharing_experience, conflict_experience, conflict_experience_type, relationship_importance, sensitivity_importance, sentiment_importance, no_preference_confidence, preference_confidence, preference_argument_confidence, additional_attributes, email, other_comments, completion_code)
select mturk_id, response_time, scenario_bundle_id, sharing_experience, conflict_experience, conflict_experience_type, relationship_importance, sensitivity_importance, sentiment_importance, no_preference_confidence, preference_confidence, preference_argument_confidence, additional_attributes, email, other_comments, completion_code from multiparty_privacy_1.turker_postsurvey_response;

# To check basic distributions

select relationship_importance, sensitivity_importance, sentiment_importance 
from turker_postsurvey_response 
into outfile '/tmp/importance.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

select no_preference_confidence,preference_confidence,preference_argument_confidence 
from turker_postsurvey_response 
into outfile '/tmp/confidence.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

select case1_policy, count(case1_policy) from turker_picturesurvey_response group by case1_policy;
select case1_policy, count(case1_policy) from turker_picturesurvey_response group by case2_policy;
select case1_policy, count(case1_policy) from turker_picturesurvey_response group by case3_policy;

select image_sentiment 
from turker_picturesurvey_response 
into outfile '/tmp/sentiment.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

select image_sensitivity 
from turker_picturesurvey_response 
into outfile '/tmp/sensitivity.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

# Low and high sensitivity images
select image_sensitivity
from turker_picturesurvey_response
where scenario_id in (select id from scenario where image_id in (1,2,5,6,9,10))
into outfile '/tmp/low_sens.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

select image_sensitivity
from turker_picturesurvey_response
where scenario_id in (select id from scenario where image_id in (3,4,7,8,11,12))
into outfile '/tmp/high_sens.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

# Low and high sentiment images
select image_sentiment
from turker_picturesurvey_response
where scenario_id in (select id from scenario where image_id in (1,3,5,7,9,11))
into outfile '/tmp/pos_senti.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

select image_sentiment
from turker_picturesurvey_response
where scenario_id in (select id from scenario where image_id in (2,4,6,8,10,12))
into outfile '/tmp/neg_senti.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

# Count agreement
select count(*) from (
  select scenario_id 
  from turker_picturesurvey_response 
  group by scenario_id 
  having group_concat(case1_policy) in (
    'a,a', 'a,a,a', 'b,b', 'b,b,b', 'c,c', 'c,c,c', 'other,other', 'other,other,other'
  )
) as T;

# Insert sentiment related data
CREATE TABLE turker_picturesurvey_response_justification_sentiment (
  id INT NOT NULL auto_increment,
  picturesurvey_id INT NOT NULL,
  case1_sentiment TINYINT NOT NULL,
  case2_sentiment TINYINT NOT NULL,
  case3_sentiment TINYINT NOT NULL,
  PRIMARY KEY (id)
);

ALTER TABLE turker_picturesurvey_response_justification_sentiment ADD INDEX picturesurvey_id (picturesurvey_id);

ALTER TABLE turker_picturesurvey_response ADD INDEX mturk_id (mturk_id);

LOAD DATA LOCAL INFILE '/home/pmuruka/Dataset/multiparty_privacy/sentiment/policy_justification_sentiment.csv' 
  INTO TABLE turker_picturesurvey_response_justification_sentiment 
  fields terminated by ',' enclosed by '"' lines terminated by '\n' 
  (picturesurvey_id, case1_sentiment, case2_sentiment, case3_sentiment);

SELECT image_sensitivity, image_sentiment,
  (CASE WHEN case1_policy = 'a' THEN 1 WHEN case1_policy = 'b' THEN 0.5 WHEN case1_policy = 'c' THEN 0 END) AS case1_policy,
  (CASE WHEN case2_policy = 'a' THEN 1 WHEN case2_policy = 'b' THEN 0.5 WHEN case2_policy = 'c' THEN 0 END) AS case2_policy,
  (CASE WHEN case3_policy = 'a' THEN 1 WHEN case3_policy = 'b' THEN 0.5 WHEN case3_policy = 'c' THEN 0 END) AS case3_policy,
  case1_sentiment, case2_sentiment, case3_sentiment
FROM turker_picturesurvey_response 
  LEFT OUTER JOIN turker_picturesurvey_response_justification_sentiment 
  ON turker_picturesurvey_response.id = turker_picturesurvey_response_justification_sentiment.picturesurvey_id
WHERE case1_policy != 'other' AND case2_policy != 'other' and case3_policy != 'other'
INTO OUTFILE '/tmp/senti.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
