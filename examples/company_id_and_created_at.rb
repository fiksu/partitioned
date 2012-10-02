#!/usr/bin/env ../spec/dummy/script/rails runner
# if you use linux, please change previous line to the
# "#! ../spec/dummy/script/rails runner"

# Before running this example you should execute "bundle install" and "rake db:create".
# To run this example you should open 'example' directory and execute example with one of the following flags:
# -C    cleanup data in database and exit;
# -F    cleanup data in database before creating new data;
#
# For example:
# ./company_id_and_created_at.rb - F

# Initial data:
#
#  Companies table is completed by four companies:
#
#  create table companies (
#        id               serial not null primary key,
#        created_at       timestamp not null default now(),
#        updated_at       timestamp,
#        name             text null
#  );
#
#  insert into companies (created_at,id,name) values
#    ('2012-03-13 13:26:52.184347',1,'Fluent Mobile, inc.'),
#    ('2012-03-13 13:26:52.184347',2,'Fiksu, inc.'),
#    ('2012-03-13 13:26:52.184347',3,'AppExchanger, inc.'),
#    ('2012-03-13 13:26:52.184347',4,'FreeMyApps, inc.');
#
#  id |         created_at         | updated_at |        name
#  ---+----------------------------+------------+---------------------
#   1 | 2012-03-11 13:26:52.184347 |            | Fluent Mobile, inc.
#   2 | 2012-03-11 13:26:52.184347 |            | Fiksu, inc.
#   3 | 2012-03-11 13:26:52.184347 |            | AppExchanger, inc.
#   4 | 2012-03-11 13:26:52.184347 |            | FreeMyApps, inc.
#
#  Employees table is associated with companies table via key - id:
#
#  create table employees (
#        id               serial not null primary key,
#        created_at       timestamp not null default now(),
#        updated_at       timestamp,
#        name             text not null,
#        salary           money not null,
#        company_id       integer not null
#  );
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
# Task:
#
#  To increase the speed of requests to the database and to reduce the time
#  of the request, need to split the Employees table to the partition tables.
#  Break criterion are company(company_id) and created date(created_at).
#
# Implementation:
#
#  Class Employee inherits from the abstract class MultiLevel,
#  which supports multilevel partitioning.
#
#  class Employee < Partitioned::MultiLevel
#
#    Indicates a relationship to the companies table.
#    belongs_to :company, :class_name => 'Company'
#
#    Create a rules for each partition.
#    Will be created on the partition for each company,
#    And will be created on the partition for each company in increments of one week.
#
#    partitioned do |partition|
#      partition.using_classes ByCompanyId, Partitioned::ByCreatedAt
#    end
#  end
#
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partition for each of company and each of company across the week:
#
#  Where partition_key_values is array of pairs company_id and date.
#  Employee.create_new_partition_tables(partition_key_values)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p1" (CHECK (( company_id = 1 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p1_20101227" (CHECK (created_at >= '2010-12-27'
#                    AND created_at < '2011-01-03')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20110103" (CHECK (created_at >= '2011-01-03'
#                    AND created_at < '2011-01-10')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20110110" (CHECK (created_at >= '2011-01-10'
#                    AND created_at < '2011-01-17')) INHERITS (employees_partitions.p1);
#  ...
#  CREATE TABLE "employees_partitions"."p1_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p1);
#  CREATE TABLE "employees_partitions"."p1_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p1);
#
#  CREATE TABLE "employees_partitions"."p2" (CHECK (( company_id = 2 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p2_20101227" (CHECK (created_at >= '2010-12-27'
#                   AND created_at < '2011-01-03')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20110103" (CHECK (created_at >= '2011-01-03'
#                   AND created_at < '2011-01-10')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20110110" (CHECK (created_at >= '2011-01-10'
#                   AND created_at < '2011-01-17')) INHERITS (employees_partitions.p2);
#  ...
#  CREATE TABLE "employees_partitions"."p2_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p2);
#  CREATE TABLE "employees_partitions"."p2_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p2);
#
#  CREATE TABLE "employees_partitions"."p3" (CHECK (( company_id = 3 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p3_20101227" (CHECK (created_at >= '2010-12-27'
#                   AND created_at < '2011-01-03')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20110103" (CHECK (created_at >= '2011-01-03'
#                   AND created_at < '2011-01-10')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20110110" (CHECK (created_at >= '2011-01-10'
#                   AND created_at < '2011-01-17')) INHERITS (employees_partitions.p3);
#  ...
#  CREATE TABLE "employees_partitions"."p3_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p3);
#  CREATE TABLE "employees_partitions"."p3_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p3);
#
#  CREATE TABLE "employees_partitions"."p4" (CHECK (( company_id = 4 ))) INHERITS (employees);
#  CREATE TABLE "employees_partitions"."p4_20101227" (CHECK (created_at >= '2010-12-27'
#                   AND created_at < '2011-01-03')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20110103" (CHECK (created_at >= '2011-01-03'
#                   AND created_at < '2011-01-10')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20110110" (CHECK (created_at >= '2011-01-10'
#                   AND created_at < '2011-01-17')) INHERITS (employees_partitions.p4);
#  ...
#  CREATE TABLE "employees_partitions"."p4_20111212" (CHECK (created_at >= '2011-12-12'
#                   AND created_at < '2011-12-19')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20111219" (CHECK (created_at >= '2011-12-19'
#                   AND created_at < '2011-12-26')) INHERITS (employees_partitions.p4);
#  CREATE TABLE "employees_partitions"."p4_20111226" (CHECK (created_at >= '2011-12-26'
#                   AND created_at < '2012-01-02')) INHERITS (employees_partitions.p4);
#
#  You should have the following tables:
#  employees_partitions.p1
#  employees_partitions.p2
#  employees_partitions.p3
#  employees_partitions.p4
#  employees_partitions.p1_20101227
#  employees_partitions.p1_20110103
#  employees_partitions.p1_20110110
#  employees_partitions.p1_20110117
#  employees_partitions.p1_20110124
#  employees_partitions.p1_20110131
#  employees_partitions.p1_20110207
#  employees_partitions.p1_20110214
#  employees_partitions.p1_20110221
#  employees_partitions.p1_20110228
#  employees_partitions.p1_20110307
#  employees_partitions.p1_20110314
#  employees_partitions.p1_20110321
#  employees_partitions.p1_20110328
#  employees_partitions.p1_20110404
#  employees_partitions.p1_20110411
#  employees_partitions.p1_20110418
#  employees_partitions.p1_20110425
#  employees_partitions.p1_20110502
#  employees_partitions.p1_20110509
#  employees_partitions.p1_20110516
#  employees_partitions.p1_20110523
#  employees_partitions.p1_20110530
#  employees_partitions.p1_20110606
#  employees_partitions.p1_20110613
#  employees_partitions.p1_20110620
#  employees_partitions.p1_20110627
#  employees_partitions.p1_20110704
#  employees_partitions.p1_20110711
#  employees_partitions.p1_20110718
#  employees_partitions.p1_20110725
#  employees_partitions.p1_20110801
#  employees_partitions.p1_20110808
#  employees_partitions.p1_20110815
#  employees_partitions.p1_20110822
#  employees_partitions.p1_20110829
#  employees_partitions.p1_20110905
#  employees_partitions.p1_20110912
#  employees_partitions.p1_20110919
#  employees_partitions.p1_20110926
#  employees_partitions.p1_20111003
#  employees_partitions.p1_20111010
#  employees_partitions.p1_20111017
#  employees_partitions.p1_20111024
#  employees_partitions.p1_20111031
#  employees_partitions.p1_20111107
#  employees_partitions.p1_20111114
#  employees_partitions.p1_20111121
#  employees_partitions.p1_20111128
#  employees_partitions.p1_20111205
#  employees_partitions.p1_20111212
#  employees_partitions.p1_20111219
#  employees_partitions.p1_20111226
#  For the next three lines the similar partitions are generated.
#  Difference only in company_id prefix.
#  employees_partitions.p2_20101227 - employees_partitions.p2_20111226
#  employees_partitions.p3_20101227 - employees_partitions.p3_20111226
#  employees_partitions.p4_20101227 - employees_partitions.p4_20111226
#
#  Each of partitions inherits from employees table,
#  thus a new row will automatically be added to the employees table .
#
#  To add data, we use the following constructions,
#  in which employees and employee_data - a random data:
#
#  create_many - allows you to add multiple records
#  Employee.create_many(employees)
#  create - allows you to add one record
#  Employee.create(employee_data)
#  new/save! - allows you to add one record without using "create" method
#  employee = Employee.new(employee_data)
#  employee.save!
#
#  For update data, we use the following constructions,
#  in which updates - a random data:
#
#  update_many - allows you to update multiple records.
#  :set_array - additional option, you may read the description
#  of the method update_many in the gem bulk_data_methods about this option.
#  Employee.update_many(updates,  { :set_array => '"salary = #{table_name}.salary +
#                                   datatable.salary, updated_at = now()"' })
#
#  This construction using for update one record. You also may use update method.
#  employee = Employee.from_partition(employee_record[:company_id],
#                                     employee_record[:created_at]).find(employee_record[:id])
#  employee.save
#
#  The data get into the employees table ONLY through partition tables.
#  You can not do an insert row into a table employees directly.
#  For this purpose special restrictions are imposed on the table employees.
#
#  Result:
#
#  We have table companies:
#
#  id |         created_at         | updated_at |        name
#  ---+----------------------------+------------+---------------------
#   1 | 2012-03-11 13:26:52.184347 |            | Fluent Mobile, inc.
#   2 | 2012-03-11 13:26:52.184347 |            | Fiksu, inc.
#   3 | 2012-03-11 13:26:52.184347 |            | AppExchanger, inc.
#   4 | 2012-03-11 13:26:52.184347 |            | FreeMyApps, inc.
#
#  Table employees with random data from 1 to 5000:
#
#  id  |     created_at      |         updated_at         |               name                |   salary    | company_id
#------+---------------------+----------------------------+-----------------------------------+-------------+------------
#    1 | 2011-03-06 21:06:59 | 2012-03-26 12:41:40.32776  | Winston J. Sillypants, I          | $125,499.00 |     3
#    2 | 2011-11-19 21:03:15 |                            | Winston J. Sillypants, II         |  $84,881.00 |     2
#    3 | 2011-10-04 10:06:14 | 2012-03-26 12:41:40.478717 | Winston J. Sillypants, III        | $124,067.00 |     3
#  ...
# 4998 | 2011-09-23 06:10:23 | 2012-03-26 11:41:30.218432 | Picholine Pimplenad, MMMMCMXCVIII | $121,474.00 |     3
# 4999 | 2011-08-26 18:24:12 | 2012-03-26 11:41:30.222835 | Picholine Pimplenad, MMMMCMXCIX   | $134,549.00 |     4
# 5000 | 2011-10-01 18:29:33 | 2012-03-26 12:41:40.544125 | Picholine Pimplenad, _V           | $135,786.00 |     1
#
#
#  Partition employees_partitions.p1 - partition where company_id = 1:
#
#  id  |     created_at      |         updated_at         |               name               |   salary    | company_id
#------+---------------------+----------------------------+----------------------------------+-------------+------------
#    5 | 2011-05-04 02:18:47 |                            | Winston J. Sillypants, V         |  $80,891.00 |      1
#    7 | 2011-08-08 03:59:22 |                            | Winston J. Sillypants, VII       | $139,737.00 |      1
#   12 | 2011-08-15 11:06:28 |                            | Winston J. Sillypants, XII       |  $73,080.00 |      1
#  ...
# 4994 | 2011-09-10 19:57:12 | 2012-03-26 11:41:30.200199 | Picholine Pimplenad, MMMMCMXCIV  | $130,988.00 |      1
# 4997 | 2011-02-12 01:19:43 | 2012-03-26 11:41:30.213843 | Picholine Pimplenad, MMMMCMXCVII |  $74,378.00 |      1
# 5000 | 2011-10-01 18:29:33 | 2012-03-26 12:41:40.544125 | Picholine Pimplenad, _V          | $135,786.00 |      1
#
#  Partition employees_partitions.p2 - partition where company_id = 2:
#
#  id  |     created_at      |         updated_at         |              name                |   salary    | company_id
#------+---------------------+----------------------------+----------------------------------+-------------+------------
#    2 | 2011-11-19 21:03:15 |                            | Winston J. Sillypants, II        |  $84,881.00 |      2
#    4 | 2011-05-10 17:00:58 |                            | Winston J. Sillypants, IV        |  $75,230.00 |      2
#    8 | 2011-03-30 01:00:17 |                            | Winston J. Sillypants, VIII      |  $69,076.00 |      2
#  ...
# 4991 | 2011-09-07 11:18:00 | 2012-03-26 11:41:30.186616 | Picholine Pimplenad, MMMMCMXCI   | $116,773.00 |      2
# 4992 | 2011-01-08 13:01:50 | 2012-03-26 11:41:31.962307 | Picholine Pimplenad, MMMMCMXCII  | $127,687.00 |      2
# 4996 | 2011-10-16 03:51:57 | 2012-03-26 11:41:36.227312 | Picholine Pimplenad, MMMMCMXCVI  |  $74,418.00 |      2
#
#  Partition employees_partitions.p3 - partition where company_id = 3:
#
#  id  |     created_at      |         updated_at         |               name               |   salary    | company_id
#------+---------------------+----------------------------+----------------------------------+-------------+------------
#    1 | 2011-03-06 21:06:59 | 2012-03-26 12:41:40.32776  | Winston J. Sillypants, I         | $125,499.00 |      3
#    3 | 2011-10-04 10:06:14 | 2012-03-26 12:41:40.478717 | Winston J. Sillypants, III       | $124,067.00 |      3
#   10 | 2011-05-29 07:14:12 |                            | Winston J. Sillypants, X         |  $63,104.00 |      3
#  ...
# 4990 | 2011-02-22 06:41:15 | 2012-03-26 11:41:30.182068 | Picholine Pimplenad, MMMMCMXC    | $113,361.00 |      3
# 4995 | 2011-12-03 05:07:19 | 2012-03-26 12:41:40.202759 | Picholine Pimplenad, MMMMCMXCV   | $114,038.00 |      3
# 4998 | 2011-09-23 06:10:23 | 2012-03-26 11:41:30.218432 | Picholine Pimplenad, MMMMCMXCVIII| $121,474.00 |      3
#
#  Partition employees_partitions.p4 - partition where company_id = 4:
#
#  id  |     created_at      |         updated_at         |               name               |   salary    | company_id
#------+---------------------+----------------------------+----------------------------------+-------------+------------
#    6 | 2011-08-04 08:24:38 | 2012-03-26 12:41:40.176512 | Winston J. Sillypants, VI        |  $77,371.00 |      4
#   13 | 2011-06-23 01:39:10 |                            | Winston J. Sillypants, XIII      |  $64,291.00 |      4
#   14 | 2011-02-23 00:58:10 |                            | Winston J. Sillypants, XIV       | $131,059.00 |      4
#  ...
# 4982 | 2011-09-19 08:05:20 | 2012-03-26 11:41:30.145857 | Picholine Pimplenad, MMMMCMLXXXII|  $66,049.00 |      4
# 4989 | 2011-02-25 11:44:56 | 2012-03-26 11:41:30.17755  | Picholine Pimplenad, MMMMCMLXXXIX| $121,402.00 |      4
# 4999 | 2011-08-26 18:24:12 | 2012-03-26 11:41:30.222835 | Picholine Pimplenad, MMMMCMXCIX  | $134,549.00 |      4
#
#  Partition employees_partitions.p1_20101227 - partition where company_id = 1
#  and created_at >= '2010-12-27 00:00:00' AND created_at < '2011-01-03 00:00:00':
#
#  id  |     created_at      |         updated_at         |               name               |   salary    | company_id
#------+---------------------+----------------------------+----------------------------------+-------------+------------
#  941 | 2011-01-01 20:26:25 |                            | Winston J. Sillypants, CMXLI     |  $89,025.00 |      1
# 1095 | 2011-01-02 22:35:39 |                            | Winston J. Sillypants, MXCV      |  $87,774.00 |      1
# 1215 | 2011-01-02 08:11:15 |                            | Winston J. Sillypants, MCCXV     | $114,288.00 |      1
# 3882 | 2011-01-02 16:06:25 | 2012-03-26 11:41:24.637678 | Jonathan Crabapple, MMMDCCCLXXXII|  $77,973.00 |      1
#
#  Partition employees_partitions.p1_20110103 - partition where company_id = 1
#  and created_at >= '2011-01-03 00:00:00' AND created_at < '2011-01-10 00:00:00':
#
#  id  |     created_at      |         updated_at         |                name                 |   salary    | company_id
#------+---------------------+----------------------------+-------------------------------------+-------------+------------
#  103 | 2011-01-07 21:16:49 |                            | Winston J. Sillypants, CIII         | $120,113.00 |    1
#  357 | 2011-01-04 05:38:30 | 2012-03-26 12:41:39.982053 | Winston J. Sillypants, CCCLVII      | $106,232.00 |    1
#  659 | 2011-01-09 11:03:37 |                            | Winston J. Sillypants, DCLIX        | $123,277.00 |    1
#  772 | 2011-01-07 19:38:57 |                            | Winston J. Sillypants, DCCLXXII     | $101,343.00 |    1
#  954 | 2011-01-03 03:54:49 | 2012-03-26 11:41:32.366725 | Winston J. Sillypants, CMLIV        |  $86,807.00 |    1
# 1392 | 2011-01-05 05:41:35 | 2012-03-26 12:41:39.982053 | Winston J. Sillypants, MCCCXCII     |  $88,342.00 |    1
# 1531 | 2011-01-07 00:27:07 |                            | Winston J. Sillypants, MDXXXI       |  $75,970.00 |    1
# 1744 | 2011-01-05 00:33:42 |                            | Winston J. Sillypants, MDCCXLIV     | $133,429.00 |    1
# 1848 | 2011-01-03 11:26:23 | 2012-03-26 12:41:39.982053 | Winston J. Sillypants, MDCCCXLVIII  | $109,972.00 |    1
# 2244 | 2011-01-08 22:40:11 | 2012-03-26 12:41:39.982053 | Winston J. Sillypants, MMCCXLIV     | $139,564.00 |    1
# 2582 | 2011-01-04 15:41:31 |                            | Winston J. Sillypants, MMDLXXXII    |  $77,274.00 |    1
# 2631 | 2011-01-09 05:36:52 |                            | Winston J. Sillypants, MMDCXXXI     |  $82,723.00 |    1
# 2695 | 2011-01-05 10:42:01 | 2012-03-26 11:41:33.452627 | Winston J. Sillypants, MMDCXCV      | $122,804.00 |    1
# 3011 | 2011-01-06 09:39:40 | 2012-03-26 11:41:20.006112 | Jonathan Crabapple, MMMXI           | $110,734.00 |    1
# 3012 | 2011-01-09 15:53:01 | 2012-03-26 11:41:34.50332  | Jonathan Crabapple, MMMXII          |  $81,894.00 |    1
# 3271 | 2011-01-08 07:13:09 | 2012-03-26 12:41:39.982053 | Jonathan Crabapple, MMMCCLXXI       | $124,440.00 |    1
# 3426 | 2011-01-06 19:18:33 | 2012-03-26 11:41:22.246979 | Jonathan Crabapple, MMMCDXXVI       | $133,000.00 |    1
# 3516 | 2011-01-09 17:37:28 | 2012-03-26 11:41:22.736326 | Jonathan Crabapple, MMMDXVI         | $133,313.00 |    1
# 3659 | 2011-01-07 09:01:39 | 2012-03-26 11:41:23.459928 | Jonathan Crabapple, MMMDCLIX        | $126,074.00 |    1
# 3694 | 2011-01-03 21:27:01 | 2012-03-26 11:41:23.637282 | Jonathan Crabapple, MMMDCXCIV       |  $77,741.00 |    1
# 4089 | 2011-01-04 00:44:02 | 2012-03-26 12:41:39.982053 | Picholine Pimplenad, MMMMLXXXIX     | $108,649.00 |    1
# 4288 | 2011-01-06 19:15:58 | 2012-03-26 11:41:26.689476 | Picholine Pimplenad, MMMMCCLXXXVIII | $119,212.00 |    1
# 4602 | 2011-01-09 09:25:28 | 2012-03-26 11:41:28.240448 | Picholine Pimplenad, MMMMDCII       | $107,141.00 |    1
# 4701 | 2011-01-04 17:08:40 | 2012-03-26 11:41:28.753281 | Picholine Pimplenad, MMMMDCCI       | $108,088.00 |    1
# 4806 | 2011-01-09 15:07:05 | 2012-03-26 12:41:39.982053 | Picholine Pimplenad, MMMMDCCCVI     | $102,352.00 |    1
#
# ...
#
#  Partition employees_partitions.p1_20111219 - partition where company_id = 1
#  and created_at >= '2011-12-19 00:00:00' AND created_at < '2011-12-26 00:00:00':
#
#  id  |     created_at      |         updated_at         |                name                |   salary    | company_id
#------+---------------------+----------------------------+------------------------------------+-------------+------------
#  426 | 2011-12-21 08:39:22 | 2012-03-26 12:41:40.484584 | Winston J. Sillypants, CDXXVI      |  $72,017.00 |     1
#  668 | 2011-12-22 10:15:37 | 2012-03-26 12:41:40.484584 | Winston J. Sillypants, DCLXVIII    | $131,558.00 |     1
# 1230 | 2011-12-21 03:35:53 |                            | Winston J. Sillypants, MCCXXX      |  $69,077.00 |     1
# 1397 | 2011-12-22 22:00:53 |                            | Winston J. Sillypants, MCCCXCVII   |  $86,444.00 |     1
# 1481 | 2011-12-19 12:14:14 |                            | Winston J. Sillypants, MCDLXXXI    | $119,621.00 |     1
# 1623 | 2011-12-22 05:27:10 |                            | Winston J. Sillypants, MDCXXIII    |  $60,037.00 |     1
# 1779 | 2011-12-20 23:13:24 |                            | Winston J. Sillypants, MDCCLXXIX   | $111,578.00 |     1
# 2180 | 2011-12-22 01:23:11 |                            | Winston J. Sillypants, MMCLXXX     |  $73,307.00 |     1
# 2192 | 2011-12-19 15:01:57 |                            | Winston J. Sillypants, MMCXCII     |  $89,245.00 |     1
# 2330 | 2011-12-20 16:17:34 |                            | Winston J. Sillypants, MMCCCXXX    |  $81,206.00 |     1
# 2434 | 2011-12-19 14:37:29 | 2012-03-26 12:41:40.484584 | Winston J. Sillypants, MMCDXXXIV   |  $97,325.00 |     1
# 2899 | 2011-12-20 12:52:28 |                            | Winston J. Sillypants, MMDCCCXCIX  | $113,013.00 |     1
# 3239 | 2011-12-25 04:16:59 | 2012-03-26 11:41:21.274165 | Jonathan Crabapple, MMMCCXXXIX     | $113,709.00 |     1
# 3368 | 2011-12-25 09:06:53 | 2012-03-26 11:41:21.958145 | Jonathan Crabapple, MMMCCCLXVIII   | $103,363.00 |     1
# 3692 | 2011-12-24 04:26:47 | 2012-03-26 11:41:37.466462 | Jonathan Crabapple, MMMDCXCII      | $135,681.00 |     1
# 3762 | 2011-12-25 05:55:07 | 2012-03-26 11:41:24.021495 | Jonathan Crabapple, MMMDCCLXII     |  $99,464.00 |     1
# 3895 | 2011-12-19 01:27:20 | 2012-03-26 11:41:24.702233 | Jonathan Crabapple, MMMDCCCXCV     |  $84,213.00 |     1
# 4092 | 2011-12-19 12:11:48 | 2012-03-26 11:41:33.112306 | Picholine Pimplenad, MMMMXCII      |  $70,843.00 |     1
# 4104 | 2011-12-19 22:34:49 | 2012-03-26 11:41:25.726682 | Picholine Pimplenad, MMMMCIV       |  $61,785.00 |     1
# 4570 | 2011-12-19 18:19:31 | 2012-03-26 11:41:28.091344 | Picholine Pimplenad, MMMMDLXX      | $110,348.00 |     1
# 4773 | 2011-12-21 20:13:59 | 2012-03-26 11:41:29.08274  | Picholine Pimplenad, MMMMDCCLXXIII | $132,882.00 |     1
# 4981 | 2011-12-22 15:54:12 | 2012-03-26 11:41:33.279564 | Picholine Pimplenad, MMMMCMLXXXI   |  $98,870.00 |     1
#
#  Partition employees_partitions.p1_20111226 - partition where company_id = 1
#  and created_at >= '2011-12-26 00:00:00' AND created_at < '2012-01-02 00:00:00':
#
#  id  |     created_at      |         updated_at         |                name                |   salary    | company_id
#------+---------------------+----------------------------+------------------------------------+-------------+------------
#  472 | 2011-12-26 02:03:03 |                            | Winston J. Sillypants, CDLXXII     | $135,441.00 |     1
#  628 | 2011-12-27 07:51:26 |                            | Winston J. Sillypants, DCXXVIII    | $110,916.00 |     1
#  752 | 2011-12-26 17:17:41 |                            | Winston J. Sillypants, DCCLII      |  $65,324.00 |     1
# 1322 | 2011-12-30 17:36:06 |                            | Winston J. Sillypants, MCCCXXII    | $129,800.00 |     1
# 1551 | 2011-12-30 13:15:13 |                            | Winston J. Sillypants, MDLI        | $121,257.00 |     1
# 1677 | 2011-12-27 00:19:32 | 2012-03-26 12:41:40.519792 | Winston J. Sillypants, MDCLXXVII   |  $91,205.00 |     1
# 1801 | 2011-12-26 23:24:22 | 2012-03-26 11:41:37.824209 | Winston J. Sillypants, MDCCCI      |  $78,008.00 |     1
# 1903 | 2011-12-27 15:09:50 | 2012-03-26 11:41:36.071137 | Winston J. Sillypants, MCMIII      |  $72,188.00 |     1
# 2207 | 2011-12-26 13:14:00 | 2012-03-26 11:41:32.551838 | Winston J. Sillypants, MMCCVII     |  $85,783.00 |     1
# 2443 | 2011-12-26 12:57:37 | 2012-03-26 11:41:37.584218 | Winston J. Sillypants, MMCDXLIII   |  $69,366.00 |     1
# 2719 | 2011-12-26 13:26:59 |                            | Winston J. Sillypants, MMDCCXIX    | $115,327.00 |     1
# 2914 | 2011-12-28 05:10:10 |                            | Winston J. Sillypants, MMCMXIV     |  $98,958.00 |     1
# 3212 | 2011-12-27 04:46:51 | 2012-03-26 11:41:21.093833 | Jonathan Crabapple, MMMCCXII       |  $75,632.00 |     1
# 3667 | 2011-12-30 18:36:50 | 2012-03-26 11:41:34.97244  | Jonathan Crabapple, MMMDCLXVII     |  $75,279.00 |     1
# 3697 | 2011-12-26 03:14:06 | 2012-03-26 11:41:23.651512 | Jonathan Crabapple, MMMDCXCVII     | $104,874.00 |     1
# 3994 | 2011-12-30 08:12:20 | 2012-03-26 11:41:25.219097 | Jonathan Crabapple, MMMCMXCIV      |  $72,999.00 |     1
# 4191 | 2011-12-30 16:10:08 | 2012-03-26 11:41:26.183611 | Picholine Pimplenad, MMMMCXCI      |  $84,720.00 |     1
# 4677 | 2011-12-28 00:49:02 | 2012-03-26 11:41:28.643459 | Picholine Pimplenad, MMMMDCLXXVII  |  $61,137.00 |     1
# 4897 | 2011-12-27 03:26:12 | 2012-03-26 11:41:32.167034 | Picholine Pimplenad, MMMMDCCCXCVII | $104,534.00 |     1
#
# A similar behavior for all other partitions where company_id = 2, 3, 4.

require File.expand_path(File.dirname(__FILE__) + "/lib/command_line_tool_mixin")
require File.expand_path(File.dirname(__FILE__) + "/lib/get_options")

include CommandLineToolMixin

$cleanup = false
$force = false
$create_many = 3000
$create_individual = 1000
$new_individual = 1000
$update_many = 1000
$update_individual = 1000

@options = {
  "--cleanup" => {
    :short => "-C",
    :argument => GetoptLong::NO_ARGUMENT,
    :note => "cleanup data in database and exit"
  },
  "--force" => {
    :short => "-F",
    :argument => GetoptLong::NO_ARGUMENT,
    :note => "cleanup data in database before creating new data"
  },
  "--create-many" => {
    :short => "-m",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to create via create_many",
    :argument_note => "NUMBER"
  },
  "--create-individual" => {
    :short => "-i",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to create via create",
    :argument_note => "NUMBER"
  },
  "--new-individual" => {
    :short => "-I",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to create via new/save",
    :argument_note => "NUMBER"
  },
  "--update-individual" => {
    :short => "-u",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to update individually",
    :argument_note => "NUMBER"
  },
  "--update-many" => {
    :short => "-U",
    :argument => GetoptLong::REQUIRED_ARGUMENT,
    :note => "how many objects to update via update_many",
    :argument_note => "NUMBER"
  },
}

command_line_options(@options) do |option,argument|
  if option == '--cleanup'
    $cleanup = true
  elsif option == '--force'
    $force = true
  elsif option == '--create-many'
    $create_many = argument.to_i
  elsif option == '--create-individual'
    $create_individual = argument.to_i
  elsif option == '--new-individual'
    $new_individual = argument.to_i
  elsif option == '--update-individual'
    $update_individual = argument.to_i
  elsif option == '--update-many'
    $update_many = argument.to_i
  end
end

if $cleanup || $force
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if $cleanup
end

$total_records = $create_many + $create_individual + $new_individual

puts "total records: #{$total_records}"

START_DATE = Date.parse('2011-01-01')
END_DATE = Date.parse('2011-12-31')

# the ActiveRecord classes
require File.expand_path(File.dirname(__FILE__) + "/lib/company")
require File.expand_path(File.dirname(__FILE__) + "/lib/by_company_id")

class Employee < Partitioned::MultiLevel
  belongs_to :company, :class_name => 'Company'
  attr_accessible :created_at, :salary, :company_id, :name

  partitioned do |partition|
    partition.using_classes ByCompanyId, Partitioned::ByCreatedAt
  end

  connection.execute <<-SQL
    create table employees
    (
        id               serial not null primary key,
        created_at       timestamp not null default now(),
        updated_at       timestamp,
        name             text not null,
        salary           money not null,
        company_id       integer not null
    );
  SQL
end

# You should have the following tables:
#  public.companies
#  public.employees

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)
dates = Partitioned::ByCreatedAt.partition_generate_range(START_DATE, END_DATE)

partition_key_values = []
company_ids.each do |company_id|
  partition_key_values << company_id
  dates.each do |date|
    partition_key_values << [company_id, date]
  end
end

# create the infrastructure for EMPLOYEES table which includes the schema
Employee.create_infrastructure

# You should have the following schema:
#  employees_partitions

# create the employees partition tables
Employee.create_new_partition_tables(partition_key_values)

# You should have the following tables:
#  employees_partitions.p1
#  employees_partitions.p2
#  employees_partitions.p3
#  employees_partitions.p4
#  employees_partitions.p1_20101227
#  employees_partitions.p1_20110103
#  employees_partitions.p1_20110110
#  employees_partitions.p1_20110117
#  employees_partitions.p1_20110124
#  employees_partitions.p1_20110131
#  employees_partitions.p1_20110207
#  employees_partitions.p1_20110214
#  employees_partitions.p1_20110221
#  employees_partitions.p1_20110228
#  employees_partitions.p1_20110307
#  employees_partitions.p1_20110314
#  employees_partitions.p1_20110321
#  employees_partitions.p1_20110328
#  employees_partitions.p1_20110404
#  employees_partitions.p1_20110411
#  employees_partitions.p1_20110418
#  employees_partitions.p1_20110425
#  employees_partitions.p1_20110502
#  employees_partitions.p1_20110509
#  employees_partitions.p1_20110516
#  employees_partitions.p1_20110523
#  employees_partitions.p1_20110530
#  employees_partitions.p1_20110606
#  employees_partitions.p1_20110613
#  employees_partitions.p1_20110620
#  employees_partitions.p1_20110627
#  employees_partitions.p1_20110704
#  employees_partitions.p1_20110711
#  employees_partitions.p1_20110718
#  employees_partitions.p1_20110725
#  employees_partitions.p1_20110801
#  employees_partitions.p1_20110808
#  employees_partitions.p1_20110815
#  employees_partitions.p1_20110822
#  employees_partitions.p1_20110829
#  employees_partitions.p1_20110905
#  employees_partitions.p1_20110912
#  employees_partitions.p1_20110919
#  employees_partitions.p1_20110926
#  employees_partitions.p1_20111003
#  employees_partitions.p1_20111010
#  employees_partitions.p1_20111017
#  employees_partitions.p1_20111024
#  employees_partitions.p1_20111031
#  employees_partitions.p1_20111107
#  employees_partitions.p1_20111114
#  employees_partitions.p1_20111121
#  employees_partitions.p1_20111128
#  employees_partitions.p1_20111205
#  employees_partitions.p1_20111212
#  employees_partitions.p1_20111219
#  employees_partitions.p1_20111226
#  For the next three lines the similar partitions are generated.
#  Difference only in company_id prefix.
#  employees_partitions.p2_20101227 - employees_partitions.p2_20111226
#  employees_partitions.p3_20101227 - employees_partitions.p3_20111226
#  employees_partitions.p4_20101227 - employees_partitions.p4_20111226

employees = []

require File.expand_path(File.dirname(__FILE__) + "/lib/roman")

# generates data for employees_partitions and employees tables


base = 0
(1..$create_many).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(base+i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

puts "creating many #{$create_many}"
Employee.create_many(employees)
base += $create_many

puts "creating individual #{$create_individual}"
(1..$create_individual).each do |i|
  employee_data = {
    :name => "Jonathan Crabapple, #{to_roman(base+i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employees << Employee.create(employee_data)
end
base += $create_individual

puts "new individual #{$new_individual}"
(1..$new_individual).each do |i|
  employee_data = {
    :name => "Picholine Pimplenad, #{to_roman(base+i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employee = Employee.new(employee_data)
  employee.save!
  employees << employee
end
base += $new_individual

updates = {}
puts "update many #{$update_many}"
(1..$update_many).each do |i|
  employee_record = employees[rand(employees.length)]
  updates[{
            :id => employee_record[:id],
            :company_id => employee_record[:company_id],
            :created_at => employee_record[:created_at]
          }] = {
      :salary => 100
  }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  employee = Employee.from_partition(employee_record[:company_id], employee_record[:created_at]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end

