#!/usr/bin/env ../spec/dummy/script/rails runner
# if you use linux, please change previous line to the
# "#! ../spec/dummy/script/rails runner"

# Before running this example you should execute "bundle install" and "rake db:create".
# To run this example you should open 'example' directory and execute example with one of the following flags:
# -C    cleanup data in database and exit;
# -F    cleanup data in database before creating new data;
#
# For example:
# ./start_date.rb - F

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
#    create table employees
#    (
#        id               serial not null primary key,
#        created_at       timestamp not null default now(),
#        updated_at       timestamp null,
#        start_date       date not null,
#        name             text not null,
#        salary           money not null,
#        company_id       integer not null
#    );
#
#   id | created_at | updated_at | start_date | name | salary | company_id
#  ----+------------+------------+------------+------+--------+------------
#
# Task:
#
#  To increase the speed of requests to the database and to reduce the time
#  of the request, need to split the Employees table to the partition tables.
#  Break criterion is a start date(start_date).
#
# Implementation:
#
#  Class Employee inherits from the abstract class ByStartDate,
#  which supports partitioning.
#
#  class Employee < Partitioned::ByStartDate
#
#    Indicates a relationship to the companies table.
#    belongs_to :company, :class_name => 'Company'
#
#    Create a rules for each partition.
#    Id is a unique index. Foreign key is company_id.
#    This imposes a restriction on each of partition, that
#    the column company_id associated with the table of companies
#    and can not have values ​​that are not in the table companies.
#    In this example, set up only 4 records in the table companies,
#    so company_id can not be equal to 5 in any partition
#    until it is an established company with id = 5.
#
#    partitioned do |partition|
#      partition.index :id, :unique => true
#      partition.foreign_key :company_id
#    end
#  end
#
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partition tables with increments of one month:
#
#  dates = Employee.partition_generate_range(START_DATE, END_DATE)
#  Employee.create_new_partition_tables(dates)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | start_date | name | salary | company_id
#  ----+------------+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p201101" (CHECK (start_date >= '2011-01-01'
#                                      AND start_date < '2011-02-01')) INHERITS (employees);
#  CREATE INDEX "p201101_start_date_idx" ON "employees_partitions"."p201101" ("start_date");
#  CREATE UNIQUE INDEX "p201101_id_udx" ON "employees_partitions"."p201101" ("id");
#  ALTER TABLE employees_partitions.p201101 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201102" (CHECK (start_date >= '2011-02-01'
#                                      AND start_date < '2011-03-01')) INHERITS (employees);
#  CREATE INDEX "p201102_start_date_idx" ON "employees_partitions"."p201102" ("start_date");
#  CREATE UNIQUE INDEX "p201102_id_udx" ON "employees_partitions"."p201102" ("id");
#  ALTER TABLE employees_partitions.p201102 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201103" (CHECK (start_date >= '2011-03-01'
#                                      AND start_date < '2011-04-01')) INHERITS (employees);
#  CREATE INDEX "p201103_start_date_idx" ON "employees_partitions"."p201103" ("start_date");
#  CREATE UNIQUE INDEX "p201103_id_udx" ON "employees_partitions"."p201103" ("id");
#  ALTER TABLE employees_partitions.p201103 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201104" (CHECK (start_date >= '2011-04-01'
#                                     AND start_date < '2011-05-01')) INHERITS (employees);
#  CREATE INDEX "p201104_start_date_idx" ON "employees_partitions"."p201104" ("start_date");
#  CREATE UNIQUE INDEX "p201104_id_udx" ON "employees_partitions"."p201104" ("id");
#  ALTER TABLE employees_partitions.p201104 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201105" (CHECK (start_date >= '2011-05-01'
#                                      AND start_date < '2011-06-01')) INHERITS (employees);
#  CREATE INDEX "p201105_start_date_idx" ON "employees_partitions"."p201105" ("start_date");
#  CREATE UNIQUE INDEX "p201105_id_udx" ON "employees_partitions"."p201105" ("id");
#  ALTER TABLE employees_partitions.p201105 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201106" (CHECK (start_date >= '2011-06-01'
#                                      AND start_date < '2011-07-01')) INHERITS (employees);
#  CREATE INDEX "p201106_start_date_idx" ON "employees_partitions"."p201106" ("start_date");
#  CREATE UNIQUE INDEX "p201106_id_udx" ON "employees_partitions"."p201106" ("id");
#  ALTER TABLE employees_partitions.p201106 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201107" (CHECK (start_date >= '2011-07-01'
#                                      AND start_date < '2011-08-01')) INHERITS (employees);
#  CREATE INDEX "p201107_start_date_idx" ON "employees_partitions"."p201107" ("start_date");
#  CREATE UNIQUE INDEX "p201107_id_udx" ON "employees_partitions"."p201107" ("id");
#  ALTER TABLE employees_partitions.p201107 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201108" (CHECK (start_date >= '2011-08-01'
#                                      AND start_date < '2011-09-01')) INHERITS (employees);
#  CREATE INDEX "p201108_start_date_idx" ON "employees_partitions"."p201108" ("start_date");
#  CREATE UNIQUE INDEX "p201108_id_udx" ON "employees_partitions"."p201108" ("id");
#  ALTER TABLE employees_partitions.p201108 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201109" (CHECK (start_date >= '2011-09-01'
#                                      AND start_date < '2011-10-01')) INHERITS (employees);
#  CREATE INDEX "p201109_start_date_idx" ON "employees_partitions"."p201109" ("start_date");
#  CREATE UNIQUE INDEX "p201109_id_udx" ON "employees_partitions"."p201109" ("id");
#  ALTER TABLE employees_partitions.p201109 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201110" (CHECK (start_date >= '2011-10-01'
#                                      AND start_date < '2011-11-01')) INHERITS (employees);
#  CREATE INDEX "p201110_start_date_idx" ON "employees_partitions"."p201110" ("start_date");
#  CREATE UNIQUE INDEX "p201110_id_udx" ON "employees_partitions"."p201110" ("id");
#  ALTER TABLE employees_partitions.p201110 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201111" (CHECK (start_date >= '2011-11-01'
#                                      AND start_date < '2011-12-01')) INHERITS (employees);
#  CREATE INDEX "p201111_start_date_idx" ON "employees_partitions"."p201111" ("start_date");
#  CREATE UNIQUE INDEX "p201111_id_udx" ON "employees_partitions"."p201111" ("id");
#  ALTER TABLE employees_partitions.p201111 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p201112" (CHECK (start_date >= '2011-12-01'
#                                            AND start_date < '2012-01-01')) INHERITS (employees);
#  CREATE INDEX "p201112_start_date_idx" ON "employees_partitions"."p201112" ("start_date");
#  CREATE UNIQUE INDEX "p201112_id_udx" ON "employees_partitions"."p201112" ("id");
#  ALTER TABLE employees_partitions.p201112 add foreign key (company_id) references companies(id);
#
# You should have the following tables with increments of one month:
#  employees_partitions.p201101
#  employees_partitions.p201102
#  employees_partitions.p201103
#  employees_partitions.p201104
#  employees_partitions.p201105
#  employees_partitions.p201106
#  employees_partitions.p201107
#  employees_partitions.p201108
#  employees_partitions.p201109
#  employees_partitions.p201110
#  employees_partitions.p201111
#  employees_partitions.p201112
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
#  Employee.update_many(updates,  {:set_array => '"salary = #{table_name}.salary +
#                                                    datatable.salary, updated_at = now()"'})
#
#  This construction using for update one record. You also may use update method.
#  employee = Employee.from_partition(employee_record[:start_date]).find(employee_record[:id])
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
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#    1 | 2012-03-27 08:00:17.328727 |                            | 2011-11-12 | Winston J. Sillypants, I          |  $61,403.00 |     4
#    2 | 2012-03-27 08:00:17.328727 |                            | 2011-11-24 | Winston J. Sillypants, II         |  $94,140.00 |     3
#    3 | 2012-03-27 08:00:17.328727 | 2012-03-27 08:00:35.840263 | 2011-07-21 | Winston J. Sillypants, III        | $101,512.00 |     3
#  ...
# 4998 | 2012-03-27 08:00:29.175598 | 2012-03-27 08:00:29.175598 | 2011-08-31 | Picholine Pimplenad, MMMMCMXCVIII |  $70,642.00 |     1
# 4999 | 2012-03-27 08:00:29.180265 | 2012-03-27 08:00:29.180265 | 2011-05-19 | Picholine Pimplenad, MMMMCMXCIX   |  $92,530.00 |     1
# 5000 | 2012-03-27 08:00:29.184878 | 2012-03-27 09:00:43.18046  | 2011-03-16 | Picholine Pimplenad, _V           | $123,110.00 |     1
#
#  Partition employees_partitions.p201101 - partition where
#  start_date >= '2011-01-01' AND start_date < '2011-02-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#    5 | 2012-03-27 08:00:17.328727 |                            | 2011-01-06 | Winston J. Sillypants, V          | $101,809.00 |     4
#   16 | 2012-03-27 08:00:17.328727 | 2012-03-27 09:00:43.198146 | 2011-01-11 | Winston J. Sillypants, XVI        | $108,194.00 |     3
#   18 | 2012-03-27 08:00:17.328727 |                            | 2011-01-29 | Winston J. Sillypants, XVIII      | $131,523.00 |     2
#  ...
# 4982 | 2012-03-27 08:00:29.099722 | 2012-03-27 08:00:29.099722 | 2011-01-12 | Picholine Pimplenad, MMMMCMLXXXII | $138,806.00 |     1
# 4990 | 2012-03-27 08:00:29.138021 | 2012-03-27 08:00:33.090801 | 2011-01-17 | Picholine Pimplenad, MMMMCMXC     |  $73,884.00 |     1
# 4997 | 2012-03-27 08:00:29.170905 | 2012-03-27 08:00:29.170905 | 2011-01-07 | Picholine Pimplenad, MMMMCMXCVII  |  $71,352.00 |     3
#
#  Partition employees_partitions.p201102 - partition where
#  start_date >= '2011-02-01' AND start_date < '2011-03-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#   39 | 2012-03-27 08:00:17.328727 | 2012-03-27 09:00:43.124392 | 2011-02-16 | Winston J. Sillypants, XXXIX      |  $61,801.00 |     3
#  118 | 2012-03-27 08:00:17.328727 |                            | 2011-02-15 | Winston J. Sillypants, CXVIII     |  $77,866.00 |     2
#  147 | 2012-03-27 08:00:17.328727 | 2012-03-27 09:00:43.124392 | 2011-02-24 | Winston J. Sillypants, CXLVII     | $119,707.00 |     4
#  ...
# 4981 | 2012-03-27 08:00:29.094862 | 2012-03-27 08:00:29.094862 | 2011-02-28 | Picholine Pimplenad, MMMMCMLXXXI  | $123,537.00 |     3
# 4993 | 2012-03-27 08:00:29.152405 | 2012-03-27 09:00:43.124392 | 2011-02-02 | Picholine Pimplenad, MMMMCMXCIII  | $139,969.00 |     3
# 4995 | 2012-03-27 08:00:29.161747 | 2012-03-27 08:00:29.161747 | 2011-02-24 | Picholine Pimplenad, MMMMCMXCV    | $114,288.00 |     3
#
#  Partition employees_partitions.p201103 - partition where
#  start_date >= '2011-03-01' AND start_date < '2011-04-01':
#
#  id  |         created_at         |         updated_at         | start_date |              name                 |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#   13 | 2012-03-27 08:00:17.328727 | 2012-03-27 08:00:30.051678 | 2011-03-17 | Winston J. Sillypants, XIII       | $127,027.00 |     2
#   20 | 2012-03-27 08:00:17.328727 |                            | 2011-03-23 | Winston J. Sillypants, XX         |  $67,804.00 |     4
#   50 | 2012-03-27 08:00:17.328727 |                            | 2011-03-12 | Winston J. Sillypants, L          |  $75,709.00 |     1
#  ...
# 4991 | 2012-03-27 08:00:29.142771 | 2012-03-27 08:00:33.477371 | 2011-03-15 | Picholine Pimplenad, MMMMCMXCI    |  $81,774.00 |     4
# 4996 | 2012-03-27 08:00:29.166363 | 2012-03-27 08:00:34.219636 | 2011-03-17 | Picholine Pimplenad, MMMMCMXCVI   |  $62,954.00 |     2
# 5000 | 2012-03-27 08:00:29.184878 | 2012-03-27 09:00:43.18046  | 2011-03-16 | Picholine Pimplenad, _V           | $123,110.00 |     1
#
#  Partition employees_partitions.p201104 - partition where
#  start_date >= '2011-04-01' AND start_date < '2011-05-01':
#
#  id  |         created_at         |         updated_at         | start_date |                name               |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#   14 | 2012-03-27 08:00:17.328727 |                            | 2011-04-26 | Winston J. Sillypants, XIV        |  $79,735.00 |     3
#   30 | 2012-03-27 08:00:17.328727 |                            | 2011-04-05 | Winston J. Sillypants, XXX        | $111,415.00 |     2
#   44 | 2012-03-27 08:00:17.328727 |                            | 2011-04-26 | Winston J. Sillypants, XLIV       | $119,897.00 |     2
#  ...
# 4953 | 2012-03-27 08:00:28.901328 | 2012-03-27 08:00:28.901328 | 2011-04-02 | Picholine Pimplenad, MMMMCMLIII   |  $79,695.00 |     4
# 4979 | 2012-03-27 08:00:29.085946 | 2012-03-27 08:00:29.085946 | 2011-04-12 | Picholine Pimplenad, MMMMCMLXXIX  | $127,600.00 |     1
# 4983 | 2012-03-27 08:00:29.105705 | 2012-03-27 08:00:30.791092 | 2011-04-04 | Picholine Pimplenad, MMMMCMLXXXIII| $136,207.00 |     1
#
#  Partition employees_partitions.p201105 - partition where
#  start_date >= '2011-05-01' AND start_date < '2011-06-01':
#
#  id  |         created_at         |         updated_at         | start_date |                name               |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#    7 | 2012-03-27 08:00:17.328727 |                            | 2011-05-06 | Winston J. Sillypants, VII        |  $92,576.00 |     3
#   41 | 2012-03-27 08:00:17.328727 | 2012-03-27 09:00:43.3198   | 2011-05-21 | Winston J. Sillypants, XLI        | $101,404.00 |     2
#   76 | 2012-03-27 08:00:17.328727 |                            | 2011-05-13 | Winston J. Sillypants, LXXVI      |  $90,599.00 |     3
#  ...
# 4987 | 2012-03-27 08:00:29.123474 | 2012-03-27 08:00:29.123474 | 2011-05-14 | Picholine Pimplenad, MMMMCMLXXXVII| $125,956.00 |     4
# 4994 | 2012-03-27 08:00:29.157211 | 2012-03-27 08:00:29.157211 | 2011-05-30 | Picholine Pimplenad, MMMMCMXCIV   |  $70,812.00 |     4
# 4999 | 2012-03-27 08:00:29.180265 | 2012-03-27 08:00:29.180265 | 2011-05-19 | Picholine Pimplenad, MMMMCMXCIX   |  $92,530.00 |     1
#
#  Partition employees_partitions.p201106 - partition where
#  start_date >= '2011-06-01' AND start_date < '2011-07-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#    9 | 2012-03-27 08:00:17.328727 |                            | 2011-06-11 | Winston J. Sillypants, IX         |  $76,361.00 |     1
#   23 | 2012-03-27 08:00:17.328727 |                            | 2011-06-07 | Winston J. Sillypants, XXIII      |  $99,101.00 |     2
#   42 | 2012-03-27 08:00:17.328727 | 2012-03-27 08:00:36.081006 | 2011-06-02 | Winston J. Sillypants, XLII       |  $98,706.00 |     1
#  ...
# 4948 | 2012-03-27 08:00:28.878517 | 2012-03-27 09:00:43.3421   | 2011-06-17 | Picholine Pimplenad, MMMMCMXLVIII |  $92,524.00 |     4
# 4969 | 2012-03-27 08:00:28.973943 | 2012-03-27 08:00:28.973943 | 2011-06-28 | Picholine Pimplenad, MMMMCMLXIX   |  $91,862.00 |     3
# 4973 | 2012-03-27 08:00:28.991697 | 2012-03-27 08:00:28.991697 | 2011-06-26 | Picholine Pimplenad, MMMMCMLXXIII | $102,840.00 |     2
#
#  Partition employees_partitions.p201107 - partition where
#  start_date >= '2011-07-01' AND start_date < '2011-08-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#    3 | 2012-03-27 08:00:17.328727 | 2012-03-27 08:00:35.840263 | 2011-07-21 | Winston J. Sillypants, III        | $101,512.00 |     3
#    4 | 2012-03-27 08:00:17.328727 |                            | 2011-07-26 | Winston J. Sillypants, IV         |  $75,210.00 |     4
#   10 | 2012-03-27 08:00:17.328727 |                            | 2011-07-16 | Winston J. Sillypants, X          |  $84,192.00 |     3
#  ...
# 4951 | 2012-03-27 08:00:28.892169 | 2012-03-27 08:00:28.892169 | 2011-07-13 | Picholine Pimplenad, MMMMCMLI     |  $74,385.00 |     1
# 4958 | 2012-03-27 08:00:28.924581 | 2012-03-27 08:00:29.740671 | 2011-07-30 | Picholine Pimplenad, MMMMCMLVIII  | $134,710.00 |     3
# 4964 | 2012-03-27 08:00:28.951394 | 2012-03-27 08:00:35.574577 | 2011-07-02 | Picholine Pimplenad, MMMMCMLXIV   | $111,258.00 |     4
#
#  Partition employees_partitions.p201108 - partition where
#  start_date >= '2011-08-01' AND start_date < '2011-09-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#   11 | 2012-03-27 08:00:17.328727 |                            | 2011-08-08 | Winston J. Sillypants, XI         | $124,558.00 |     2
#   28 | 2012-03-27 08:00:17.328727 | 2012-03-27 09:00:43.087144 | 2011-08-12 | Winston J. Sillypants, XXVIII     | $103,416.00 |     2
#   37 | 2012-03-27 08:00:17.328727 |                            | 2011-08-30 | Winston J. Sillypants, XXXVII     | $119,090.00 |     2
#  ...
# 4985 | 2012-03-27 08:00:29.114785 | 2012-03-27 08:00:29.114785 | 2011-08-12 | Picholine Pimplenad, MMMMCMLXXXV  |  $79,474.00 |     3
# 4989 | 2012-03-27 08:00:29.133224 | 2012-03-27 08:00:35.107938 | 2011-08-29 | Picholine Pimplenad, MMMMCMLXXXIX | $118,545.00 |     2
# 4998 | 2012-03-27 08:00:29.175598 | 2012-03-27 08:00:29.175598 | 2011-08-31 | Picholine Pimplenad, MMMMCMXCVIII |  $70,642.00 |     1
#
#  Partition employees_partitions.p201109 - partition where
#  start_date >= '2011-09-01' AND start_date < '2011-10-01':
#
#  id  |         created_at         |         updated_at         | start_date |                name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+------------------------------------+-------------+------------
#   17 | 2012-03-27 08:00:17.328727 |                            | 2011-09-03 | Winston J. Sillypants, XVII        | $137,286.00 |     4
#   22 | 2012-03-27 08:00:17.328727 |                            | 2011-09-19 | Winston J. Sillypants, XXII        | $108,640.00 |     1
#   24 | 2012-03-27 08:00:17.328727 |                            | 2011-09-09 | Winston J. Sillypants, XXIV        |  $71,643.00 |     2
#  ...
# 4967 | 2012-03-27 08:00:28.965141 | 2012-03-27 08:00:28.965141 | 2011-09-15 | Picholine Pimplenad, MMMMCMLXVII   | $103,039.00 |     1
# 4978 | 2012-03-27 08:00:29.015989 | 2012-03-27 09:00:43.299117 | 2011-09-26 | Picholine Pimplenad, MMMMCMLXXVIII | $110,453.00 |     3
# 4988 | 2012-03-27 08:00:29.12854  | 2012-03-27 08:00:34.414851 | 2011-09-15 | Picholine Pimplenad, MMMMCMLXXXVIII| $140,014.00 |     4
#
#  Partition employees_partitions.p201110 - partition where
#  start_date >= '2011-10-01' AND start_date < '2011-11-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#   15 | 2012-03-27 08:00:17.328727 |                            | 2011-10-29 | Winston J. Sillypants, XV         | $122,755.00 |     4
#   27 | 2012-03-27 08:00:17.328727 |                            | 2011-10-18 | Winston J. Sillypants, XXVII      |  $62,954.00 |     3
#   38 | 2012-03-27 08:00:17.328727 |                            | 2011-10-14 | Winston J. Sillypants, XXXVIII    |  $85,840.00 |     1
#  ...
# 4976 | 2012-03-27 08:00:29.005547 | 2012-03-27 08:00:29.005547 | 2011-10-10 | Picholine Pimplenad, MMMMCMLXXVI  | $125,095.00 |     3
# 4984 | 2012-03-27 08:00:29.11038  | 2012-03-27 08:00:33.269888 | 2011-10-11 | Picholine Pimplenad, MMMMCMLXXXIV | $105,346.00 |     3
# 4992 | 2012-03-27 08:00:29.147872 | 2012-03-27 08:00:29.147872 | 2011-10-08 | Picholine Pimplenad, MMMMCMXCII   |  $65,541.00 |     4
#
#  Partition employees_partitions.p201111 - partition where
#  start_date >= '2011-11-01' AND start_date < '2011-12-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#    1 | 2012-03-27 08:00:17.328727 |                            | 2011-11-12 | Winston J. Sillypants, I          |  $61,403.00 |     4
#    2 | 2012-03-27 08:00:17.328727 |                            | 2011-11-24 | Winston J. Sillypants, II         |  $94,140.00 |     3
#    8 | 2012-03-27 08:00:17.328727 |                            | 2011-11-22 | Winston J. Sillypants, VIII       | $139,234.00 |     1
#  ...
# 4950 | 2012-03-27 08:00:28.887451 | 2012-03-27 08:00:28.887451 | 2011-11-18 | Picholine Pimplenad, MMMMCML      |  $78,791.00 |     2
# 4962 | 2012-03-27 08:00:28.942506 | 2012-03-27 09:00:43.14307  | 2011-11-15 | Picholine Pimplenad, MMMMCMLXII   | $121,325.00 |     3
# 4974 | 2012-03-27 08:00:28.996189 | 2012-03-27 08:00:28.996189 | 2011-11-21 | Picholine Pimplenad, MMMMCMLXXIV  |  $60,607.00 |     2
#
#  Partition employees_partitions.p201112 - partition where
#  start_date >= '2011-12-01' AND start_date < '2012-01-01':
#
#  id  |         created_at         |         updated_at         | start_date |               name                |   salary    | company_id
#------+----------------------------+----------------------------+------------+-----------------------------------+-------------+------------
#    6 | 2012-03-27 08:00:17.328727 |                            | 2011-12-22 | Winston J. Sillypants, VI         | $114,347.00 |     2
#   12 | 2012-03-27 08:00:17.328727 |                            | 2011-12-22 | Winston J. Sillypants, XII        |  $83,008.00 |     4
#   19 | 2012-03-27 08:00:17.328727 | 2012-03-27 08:00:35.479746 | 2011-12-16 | Winston J. Sillypants, XIX        | $101,796.00 |     4
#  ...
# 4939 | 2012-03-27 08:00:28.837839 | 2012-03-27 09:00:43.1619   | 2011-12-16 | Picholine Pimplenad, MMMMCMXXXIX  | $114,202.00 |     1
# 4968 | 2012-03-27 08:00:28.969687 | 2012-03-27 08:00:33.652371 | 2011-12-13 | Picholine Pimplenad, MMMMCMLXVIII | $116,727.00 |     3
# 4975 | 2012-03-27 08:00:29.000734 | 2012-03-27 08:00:29.000734 | 2011-12-25 | Picholine Pimplenad, MMMMCMLXXV   | $125,930.00 |     2
#

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
    :note => "how many objects to update indivudually",
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

class Partitioned::ByStartDate < Partitioned::ByMonthlyTimeField
  self.abstract_class = true

  def self.partition_time_field
    return :start_date
  end
end

class Employee < Partitioned::ByStartDate
  belongs_to :company, :class_name => 'Company'
  attr_accessible :company_id, :start_date, :salary, :name

  partitioned do |partition|
    partition.index :id, :unique => true
    partition.foreign_key :company_id
  end

  connection.execute <<-SQL
    create table employees
    (
        id               serial not null primary key,
        created_at       timestamp not null default now(),
        updated_at       timestamp null,
        start_date       date not null,
        name             text not null,
        salary           money not null,
        company_id       integer not null
    );
  SQL
end

# You should have the following tables:
#  public.companies
#  public.employees

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# You should have the following schema:
#  employees_partitions

dates = Employee.partition_generate_range(START_DATE, END_DATE)
Employee.create_new_partition_tables(dates)

# You should have the following tables with increments of one month:
#  employees_partitions.p201101
#  employees_partitions.p201102
#  employees_partitions.p201103
#  employees_partitions.p201104
#  employees_partitions.p201105
#  employees_partitions.p201106
#  employees_partitions.p201107
#  employees_partitions.p201108
#  employees_partitions.p201109
#  employees_partitions.p201110
#  employees_partitions.p201111
#  employees_partitions.p201112

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

# now add some employees across the year.

employees = []

require File.expand_path(File.dirname(__FILE__) + "/lib/roman")

# generates data for employees_partitions and employees tables

base = 0
(1..$create_many).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(base+i)}",
    :start_date => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
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
    :start_date => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
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
    :start_date => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employee = Employee.new(employee_data)
  employee.save
  employees << employee
end
base += $new_individual

updates = {}
puts "update many #{$update_many}"
(1..$update_many).each do |i|
  index = rand(employees.length)
  employee_record = employees[index]
  updates[{
            :id => employee_record[:id],
            :start_date => employee_record[:start_date]
          }] = {
    :salary => 100
  }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  index = rand(employees.length)
  employee_record = employees[index]
  employee = Employee.from_partition(employee_record[:start_date]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end
