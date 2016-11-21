DROP TABLE if exists medintsev.MA1730_partners_conversion;
create external table medintsev.MA1730_partners_conversion (
    clid int,
  num_purchases int,
  conversion double
)
row format delimited
        fields terminated by ';'
        lines terminated by '\n'
stored as textfile
location '/user/medintsev/MARKETANSWERS-1730/data';