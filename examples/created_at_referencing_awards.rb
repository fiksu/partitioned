#!/usr/bin/env ../spec/dummy/script/rails runner
# if you use linux, please change previous line to the
# "#! ../spec/dummy/script/rails runner"

# Before running this example you should execute "bundle install" and "rake db:create".
# To run this example you should open 'example' directory and execute example with one of the following flags:
# -C    cleanup data in database and exit;
# -F    cleanup data in database before creating new data;
#
# For example:
# ./created_at_referencing_awards.rb - F

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
#  Awards table is associated with employees table via key - employee_id:
#
#  create table awards (
#        id                    serial not null primary key,
#        created_at            timestamp not null default now(),
#        employee_created_at   timestamp not null,
#        awarded_on            date not null,
#        employee_id           integer not null,
#        award_title           text not null
#  );
#
#  id | created_at | employee_created_at | awarded_on | employee_id | award_title
# ----+------------+---------------------+------------+-------------+-------------
#
# Task:
#
#  To increase the speed of requests to the database and to reduce the time
#  of the request, need to split the Employees table and Awards table to the partition tables.
#  Break criterion for Employees table is a created date(created_at).
#  Break criterion for Awards table is a employee created date(employee_created_at).
#
# Implementation:
#
#  Class Employee inherits from the abstract class ByCreatedAt,
#  which supports partitioning.
#
#  class Employee < Partitioned::ByCreatedAt
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
#  Class Award inherits from the abstract class ByEmployeeCreatedAt,
#  which supports partitioning.
#
#  class Award < ByEmployeeCreatedAt
#
#    Indicates a relationship to the employees table.
#    belongs_to :employee, :class_name => 'Employee'
#
#    partitioned do |partition|
#      partition.foreign_key lambda {|model, *partition_key_values|
#        return Configurator::Data::ForeignKey.
#                    new(:employee_id, Employee.partition_table_name(*partition_key_values), :id)
#      }
#    end
#  end
#
#  Create a schema employees_partitions, within which to store employees partitions:
#
#  Employee.create_infrastructure
#
#  Create the infrastructure for Awards table which includes the schema and partition tables:
#
#  Award.create_infrastructure
#
#  Create a partition tables with increments of one week:
#
#  dates = Employee.partition_generate_range(START_DATE, END_DATE)
#
#  Employee.create_new_partition_tables(dates)
#  Each of employees partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p20101227" (CHECK (created_at >= '2010-12-27' AND
#                                        created_at < '2011-01-03')) INHERITS (employees);
#  CREATE INDEX "p20101227_created_at_idx" ON "employees_partitions"."p20101227" ("created_at");
#  CREATE UNIQUE INDEX "p20101227_id_udx" ON "employees_partitions"."p20101227" ("id");
#  ALTER TABLE employees_partitions.p20101227 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20110103" (CHECK (created_at >= '2011-01-03' AND
#                                        created_at < '2011-01-10')) INHERITS (employees);
#  CREATE INDEX "p20110103_created_at_idx" ON "employees_partitions"."p20110103" ("created_at");
#  CREATE UNIQUE INDEX "p20110103_id_udx" ON "employees_partitions"."p20110103" ("id");
#  ALTER TABLE employees_partitions.p20110103 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20110110" (CHECK (created_at >= '2011-01-10' AND
#                                        created_at < '2011-01-17')) INHERITS (employees);
#  CREATE INDEX "p20110110_created_at_idx" ON "employees_partitions"."p20110110" ("created_at");
#  CREATE UNIQUE INDEX "p20110110_id_udx" ON "employees_partitions"."p20110110" ("id");
#  ALTER TABLE employees_partitions.p20110110 add foreign key (company_id) references companies(id);
#
#  ...
#
#  CREATE TABLE "employees_partitions"."p20111212" (CHECK (created_at >= '2011-12-12' AND
#                                        created_at < '2011-12-19')) INHERITS (employees);
#  CREATE INDEX "p20111212_created_at_idx" ON "employees_partitions"."p20111212" ("created_at");
#  CREATE UNIQUE INDEX "p20111212_id_udx" ON "employees_partitions"."p20111212" ("id");
#  ALTER TABLE employees_partitions.p20111212 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20111219" (CHECK (created_at >= '2011-12-19' AND
#                                     created_at < '2011-12-26')) INHERITS (employees);
#  CREATE INDEX "p20111219_created_at_idx" ON "employees_partitions"."p20111219" ("created_at");
#  CREATE UNIQUE INDEX "p20111219_id_udx" ON "employees_partitions"."p20111219" ("id");
#  ALTER TABLE employees_partitions.p20111219 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20111226" (CHECK (created_at >= '2011-12-26' AND
#                                     created_at < '2012-01-02')) INHERITS (employees);
#  CREATE INDEX "p20111226_created_at_idx" ON "employees_partitions"."p20111226" ("created_at");
#  CREATE UNIQUE INDEX "p20111226_id_udx" ON "employees_partitions"."p20111226" ("id");
#  ALTER TABLE employees_partitions.p20111226 add foreign key (company_id) references companies(id);
#
# You should have the following tables with increments of one week:
#  employees_partitions.p20101227
#  employees_partitions.p20110103
#  employees_partitions.p20110110
#  employees_partitions.p20110117
#  employees_partitions.p20110124
#  employees_partitions.p20110131
#  employees_partitions.p20110207
#  employees_partitions.p20110214
#  employees_partitions.p20110221
#  employees_partitions.p20110228
#  employees_partitions.p20110307
#  employees_partitions.p20110314
#  employees_partitions.p20110321
#  employees_partitions.p20110328
#  employees_partitions.p20110404
#  employees_partitions.p20110411
#  employees_partitions.p20110418
#  employees_partitions.p20110425
#  employees_partitions.p20110502
#  employees_partitions.p20110509
#  employees_partitions.p20110516
#  employees_partitions.p20110523
#  employees_partitions.p20110530
#  employees_partitions.p20110606
#  employees_partitions.p20110613
#  employees_partitions.p20110620
#  employees_partitions.p20110627
#  employees_partitions.p20110704
#  employees_partitions.p20110711
#  employees_partitions.p20110718
#  employees_partitions.p20110725
#  employees_partitions.p20110801
#  employees_partitions.p20110808
#  employees_partitions.p20110815
#  employees_partitions.p20110822
#  employees_partitions.p20110829
#  employees_partitions.p20110905
#  employees_partitions.p20110912
#  employees_partitions.p20110919
#  employees_partitions.p20110926
#  employees_partitions.p20111003
#  employees_partitions.p20111010
#  employees_partitions.p20111017
#  employees_partitions.p20111024
#  employees_partitions.p20111031
#  employees_partitions.p20111107
#  employees_partitions.p20111114
#  employees_partitions.p20111121
#  employees_partitions.p20111128
#  employees_partitions.p20111205
#  employees_partitions.p20111212
#  employees_partitions.p20111219
#  employees_partitions.p20111226
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
#  Employee.update_many(updates, { :set_array => '"salary = #{table_name}.salary +
#                                                    datatable.salary, updated_at = now()"' })
#
#  This construction using for update one record. You also may use update method.
#  employee = Employee.from_partition(employee_record[:created_at]).find(employee_record[:id])
#  employee.save
#
#  The data get into the employees table ONLY through partition tables.
#  You can not do an insert row into a table employees directly.
#  For this purpose special restrictions are imposed on the table employees.
#
#  Award.create_new_partition_tables(dates)
#  Each of awards partition has the same structure as that of the awards table:
#
#   id  | created_at | employee_created_at | awarded_on | employee_id | award_title
#  -----+------------+---------------------+------------+-------------+-------------
#
#  CREATE TABLE "awards_partitions"."p20101227" (CHECK (employee_created_at >= '2010-12-27' AND
#                                                       employee_created_at < '2011-01-03')) INHERITS (awards);
#  CREATE INDEX "p20101227_employee_created_at_idx" ON "awards_partitions"."p20101227" ("employee_created_at");
#  ALTER TABLE awards_partitions.p20101227 add foreign key (employee_id) references employees_partitions.p20101227(id);
#
#  CREATE TABLE "awards_partitions"."p20110103" (CHECK (employee_created_at >= '2011-01-03' AND
#                                                       employee_created_at < '2011-01-10')) INHERITS (awards);
#  CREATE INDEX "p20110103_employee_created_at_idx" ON "awards_partitions"."p20110103" ("employee_created_at");
#  ALTER TABLE awards_partitions.p20110103 add foreign key (employee_id) references employees_partitions.p20110103(id);
#
#  CREATE TABLE "awards_partitions"."p20110110" (CHECK (employee_created_at >= '2011-01-10' AND
#                                                       employee_created_at < '2011-01-17')) INHERITS (awards);
#  CREATE INDEX "p20110110_employee_created_at_idx" ON "awards_partitions"."p20110110" ("employee_created_at");
#  ALTER TABLE awards_partitions.p20110110 add foreign key (employee_id) references employees_partitions.p20110110(id);
#
#  ...
#
#  CREATE TABLE "awards_partitions"."p20111212" (CHECK (employee_created_at >= '2011-12-12' AND
#                                                       employee_created_at < '2011-12-19')) INHERITS (awards);
#  CREATE INDEX "p20111212_employee_created_at_idx" ON "awards_partitions"."p20111212" ("employee_created_at");
#  ALTER TABLE awards_partitions.p20111212 add foreign key (employee_id) references employees_partitions.p20111212(id);
#
#  CREATE TABLE "awards_partitions"."p20111219" (CHECK (employee_created_at >= '2011-12-19' AND
#                                                       employee_created_at < '2011-12-26')) INHERITS (awards);
#  CREATE INDEX "p20111219_employee_created_at_idx" ON "awards_partitions"."p20111219" ("employee_created_at");
#  ALTER TABLE awards_partitions.p20111219 add foreign key (employee_id) references employees_partitions.p20111219(id);
#
#  CREATE TABLE "awards_partitions"."p20111226" (CHECK (employee_created_at >= '2011-12-26' AND
#                                                       employee_created_at < '2012-01-02')) INHERITS (awards);
#  CREATE INDEX "p20111226_employee_created_at_idx" ON "awards_partitions"."p20111226" ("employee_created_at");
#  ALTER TABLE awards_partitions.p20111226 add foreign key (employee_id) references employees_partitions.p20111226(id);
#
# You should have the following tables with increments of one week:
#  awards_partitions.p20101227
#  awards_partitions.p20110103
#  awards_partitions.p20110110
#  awards_partitions.p20110117
#  awards_partitions.p20110124
#  awards_partitions.p20110131
#  awards_partitions.p20110207
#  awards_partitions.p20110214
#  awards_partitions.p20110221
#  awards_partitions.p20110228
#  awards_partitions.p20110307
#  awards_partitions.p20110314
#  awards_partitions.p20110321
#  awards_partitions.p20110328
#  awards_partitions.p20110404
#  awards_partitions.p20110411
#  awards_partitions.p20110418
#  awards_partitions.p20110425
#  awards_partitions.p20110502
#  awards_partitions.p20110509
#  awards_partitions.p20110516
#  awards_partitions.p20110523
#  awards_partitions.p20110530
#  awards_partitions.p20110606
#  awards_partitions.p20110613
#  awards_partitions.p20110620
#  awards_partitions.p20110627
#  awards_partitions.p20110704
#  awards_partitions.p20110711
#  awards_partitions.p20110718
#  awards_partitions.p20110725
#  awards_partitions.p20110801
#  awards_partitions.p20110808
#  awards_partitions.p20110815
#  awards_partitions.p20110822
#  awards_partitions.p20110829
#  awards_partitions.p20110905
#  awards_partitions.p20110912
#  awards_partitions.p20110919
#  awards_partitions.p20110926
#  awards_partitions.p20111003
#  awards_partitions.p20111010
#  awards_partitions.p20111017
#  awards_partitions.p20111024
#  awards_partitions.p20111031
#  awards_partitions.p20111107
#  awards_partitions.p20111114
#  awards_partitions.p20111121
#  awards_partitions.p20111128
#  awards_partitions.p20111205
#  awards_partitions.p20111212
#  awards_partitions.p20111219
#  awards_partitions.p20111226
#
#  Each of partitions inherits from awards table,
#  thus a new row will automatically be added to the awards table .
#
#  To add data, we use the following constructions,
#  in which awards and award_data - a random data:
#
#  create_many - allows you to add multiple records
#  Award.create_many(awards)
#  create - allows you to add one record
#  Award.create(award_data)
#  new/save! - allows you to add one record without using "create" method
#  award = Award.new(award_data)
#  award.save!
#
#  For update data, we use the following constructions,
#  in which updates - a random data:
#
#  update_many - allows you to update multiple records.
#  :set_array - additional option, you may read the description
#  of the method update_many in the gem bulk_data_methods about this option.
#  Award.update_many(updates, { :set_array => '"award_title = datatable.award_title,
#                                                            awarded_on = now()"' })
#
#  This construction using for update one record. You also may use update method.
#  award = Award.from_partition(award_record[:employee_created_at]).find(award_record[:id])
#  award.save
#
#  The data get into the awards table ONLY through partition tables.
#  You can not do an insert row into a table awards directly.
#  For this purpose special restrictions are imposed on the table awards.
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
#  Table employees with random data from 1 to 2500:
#
#  id  |     created_at      |         updated_at         |              name               |   salary    | company_id
#------+---------------------+----------------------------+---------------------------------+-------------+------------
#    1 | 2011-02-27 09:30:12 |                            | Winston J. Sillypants, I        |  $89,653.00 |     2
#    2 | 2011-02-08 01:08:50 |                            | Winston J. Sillypants, II       |  $94,759.00 |     4
#    3 | 2011-12-08 19:54:21 | 2012-03-27 11:38:50.674123 | Winston J. Sillypants, III      |  $73,726.00 |     2
#  ...
# 2498 | 2011-01-18 01:43:42 | 2012-03-27 10:38:36.029443 | Picholine Pimplenad, MMCDXCVIII |  $61,107.00 |     3
# 2499 | 2011-11-03 21:07:05 | 2012-03-27 11:38:50.656815 | Picholine Pimplenad, MMCDXCIX   |  $62,638.00 |     4
# 2500 | 2011-12-26 04:03:57 | 2012-03-27 10:38:36.039419 | Picholine Pimplenad, MMD        | $102,855.00 |     1
#
#  Table awards with random data from 1 to 2500:
#
#  id  |         created_at         | employee_created_at | awarded_on | employee_id |           award_title
#------+----------------------------+---------------------+------------+-------------+---------------------------------
#    1 | 2012-03-27 10:38:39.728924 | 2011-10-17 13:44:07 | 2012-07-02 |         440 | You're the best I
#    2 | 2012-03-27 10:38:39.728924 | 2011-06-15 20:00:02 | 2012-06-07 |         921 | You're the best II
#    3 | 2012-03-27 10:38:39.728924 | 2011-11-20 06:30:34 | 2012-10-16 |         349 | You're the best III
#  ...
# 2498 | 2012-03-27 10:38:45.954887 | 2011-10-09 22:51:04 | 2012-07-10 |        1421 | You're the best MMCDXCVIII
# 2499 | 2012-03-27 10:38:45.959831 | 2011-01-07 02:25:00 | 2011-09-07 |        1717 | You're the best MMCDXCIX
# 2500 | 2012-03-27 10:38:45.964599 | 2011-08-23 08:02:50 | 2012-03-27 |         515 | You're the greatest DCCCXXXVIII
#
#  Partition employees_partitions.p20101227 - partition where
#  created_at >= '2010-12-27 00:00:00' AND created_at < '2011-01-03 00:00:00':
#
#  id  |     created_at      |         updated_at         |              name               |   salary    | company_id
#------+---------------------+----------------------------+---------------------------------+-------------+------------
#  641 | 2011-01-01 06:32:16 |                            | Winston J. Sillypants, DCXLI    | $103,834.00 |     4
#  644 | 2011-01-02 08:00:35 | 2012-03-27 11:38:50.70092  | Winston J. Sillypants, DCXLIV   |  $98,756.00 |     4
#  766 | 2011-01-01 18:22:34 | 2012-03-27 11:38:50.70092  | Winston J. Sillypants, DCCLXVI  | $133,384.00 |     1
#  ...
# 2058 | 2011-01-01 09:07:52 | 2012-03-27 10:38:33.742273 | Picholine Pimplenad, MMLVIII    | $130,422.00 |     3
# 2422 | 2011-01-01 07:44:38 | 2012-03-27 10:38:35.604894 | Picholine Pimplenad, MMCDXXII   |  $86,484.00 |     3
# 2482 | 2011-01-01 07:52:26 | 2012-03-27 10:38:35.890074 | Picholine Pimplenad, MMCDLXXXII | $113,265.00 |     2
#
#  Partition employees_partitions.p20110103 - partition where
#  created_at >= '2011-01-03 00:00:00' AND created_at < '2011-01-10 00:00:00':
#
#  id  |     created_at      |         updated_at         |              name               |   salary    | company_id
#------+---------------------+----------------------------+---------------------------------+-------------+------------
#   22 | 2011-01-09 03:20:24 | 2012-03-27 11:38:50.588609 | Winston J. Sillypants, XXII     | $139,538.00 |     2
#   63 | 2011-01-09 04:51:41 |                            | Winston J. Sillypants, LXIII    | $127,878.00 |     2
#  282 | 2011-01-03 09:19:16 | 2012-03-27 10:38:37.896505 | Winston J. Sillypants, CCLXXXII | $106,733.00 |     3
#  ...
# 2366 | 2011-01-07 19:18:54 | 2012-03-27 11:38:50.588609 | Picholine Pimplenad, MMCCCLXVI  | $112,775.00 |     4
# 2392 | 2011-01-08 21:19:03 | 2012-03-27 10:38:35.463625 | Picholine Pimplenad, MMCCCXCII  | $106,327.00 |     4
# 2399 | 2011-01-03 06:04:39 | 2012-03-27 10:38:35.497058 | Picholine Pimplenad, MMCCCXCIX  |  $77,089.00 |     2
#
#  Partition employees_partitions.p20110110 - partition where
#  created_at >= '2011-01-10 00:00:00' AND created_at < '2011-01-17 00:00:00':
#
#  id  |     created_at      |         updated_at         |              name               |   salary    | company_id
#------+---------------------+----------------------------+---------------------------------+-------------+------------
#   30 | 2011-01-15 11:53:36 |                            | Winston J. Sillypants, XXX      | $130,203.00 |     4
#   34 | 2011-01-16 20:01:25 | 2012-03-27 10:38:37.428899 | Winston J. Sillypants, XXXIV    |  $69,225.00 |     4
#  109 | 2011-01-12 08:17:06 |                            | Winston J. Sillypants, CIX      | $127,294.00 |     4
#  ...
# 2325 | 2011-01-13 04:17:58 | 2012-03-27 11:38:50.661251 | Picholine Pimplenad, MMCCCXXV   |  $98,912.00 |     2
# 2362 | 2011-01-14 06:48:52 | 2012-03-27 10:38:35.316907 | Picholine Pimplenad, MMCCCLXII  | $116,023.00 |     2
# 2403 | 2011-01-12 00:28:05 | 2012-03-27 10:38:38.022877 | Picholine Pimplenad, MMCDIII    | $106,252.00 |     3
#
# ...
#
#  Partition employees_partitions.p20111212 - partition where
#  created_at >= '2011-12-12 00:00:00' AND created_at < '2011-12-19 00:00:00':
#
#  id  |     created_at      |         updated_at         |               name                |   salary    | company_id
#------+---------------------+----------------------------+-----------------------------------+-------------+------------
#   85 | 2011-12-15 00:35:46 |                            | Winston J. Sillypants, LXXXV      | $110,469.00 |     2
#  137 | 2011-12-12 10:58:14 |                            | Winston J. Sillypants, CXXXVII    |  $96,201.00 |     1
#  222 | 2011-12-14 07:13:52 | 2012-03-27 11:38:50.556991 | Winston J. Sillypants, CCXXII     | $127,907.00 |     4
#  ...
# 2351 | 2011-12-17 17:04:37 | 2012-03-27 10:38:35.26375  | Picholine Pimplenad, MMCCCLI      |  $82,418.00 |     3
# 2383 | 2011-12-12 22:01:11 | 2012-03-27 10:38:35.420221 | Picholine Pimplenad, MMCCCLXXXIII |  $62,355.00 |     2
# 2391 | 2011-12-18 02:42:22 | 2012-03-27 10:38:35.458827 | Picholine Pimplenad, MMCCCXCI     | $116,403.00 |     4
#
#  Partition employees_partitions.p20111219 - partition where
#  created_at >= '2011-12-19 00:00:00' AND created_at < '2011-12-26 00:00:00':
#
#  id  |     created_at      |         updated_at         |               name               |   salary    | company_id
#------+---------------------+----------------------------+----------------------------------+-------------+------------
#    7 | 2011-12-20 04:40:07 | 2012-03-27 11:38:50.492916 | Winston J. Sillypants, VII       |  $89,608.00 |      4
#   84 | 2011-12-25 14:45:16 |                            | Winston J. Sillypants, LXXXIV    | $122,661.00 |      2
#  120 | 2011-12-19 21:18:09 |                            | Winston J. Sillypants, CXX       | $116,477.00 |      3
#  ...
# 2256 | 2011-12-22 22:28:13 | 2012-03-27 10:38:34.755228 | Picholine Pimplenad, MMCCLVI     | $106,724.00 |      1
# 2384 | 2011-12-23 13:27:09 | 2012-03-27 10:38:35.425007 | Picholine Pimplenad, MMCCCLXXXIV | $107,816.00 |      2
# 2434 | 2011-12-24 05:43:52 | 2012-03-27 10:38:35.65791  | Picholine Pimplenad, MMCDXXXIV   |  $69,897.00 |      1
#
#  Partition employees_partitions.p20111226 - partition where
#  created_at >= '2011-12-26 00:00:00' AND created_at < '2012-01-02 00:00:00':
#
#  id  |     created_at      |         updated_at         |             name              |   salary    | company_id
#------+---------------------+----------------------------+-------------------------------+-------------+------------
#    4 | 2011-12-28 09:59:33 |                            | Winston J. Sillypants, IV     |  $75,740.00 |     3
#   33 | 2011-12-26 00:15:53 |                            | Winston J. Sillypants, XXXIII |  $72,866.00 |     4
#  134 | 2011-12-30 22:29:57 |                            | Winston J. Sillypants, CXXXIV | $101,256.00 |     2
#  ...
# 2417 | 2011-12-26 14:49:13 | 2012-03-27 10:38:35.584125 | Picholine Pimplenad, MMCDXVII | $111,609.00 |     1
# 2420 | 2011-12-30 22:15:29 | 2012-03-27 10:38:35.596825 | Picholine Pimplenad, MMCDXX   | $105,180.00 |     2
# 2500 | 2011-12-26 04:03:57 | 2012-03-27 10:38:36.039419 | Picholine Pimplenad, MMD      | $102,855.00 |     1
#
#  Partition awards_partitions.p20101227 - partition where
#  employee_created_at >= '2010-12-27 00:00:00' AND employee_created_at < '2011-01-03 00:00:00':
#
#  id  |         created_at         | employee_created_at | awarded_on | employee_id |        award_title
#------+----------------------------+---------------------+------------+-------------+----------------------------
#   33 | 2012-03-27 10:38:39.728924 | 2011-01-02 05:58:21 | 2011-09-19 |        1811 | You're the best XXXIII
#  259 | 2012-03-27 10:38:39.728924 | 2011-01-02 10:54:32 | 2011-03-23 |         873 | You're the best CCLIX
#  261 | 2012-03-27 10:38:39.728924 | 2011-01-02 10:54:32 | 2011-12-10 |         873 | You're the best CCLXI
#  ...
# 2269 | 2012-03-27 10:38:44.743054 | 2011-01-02 05:58:21 | 2012-03-27 |        1811 | You're the greatest CDXV
# 2408 | 2012-03-27 10:38:45.458391 | 2011-01-01 06:32:16 | 2011-06-25 |         641 | You're the best MMCDVIII
# 2484 | 2012-03-27 10:38:45.887678 | 2011-01-02 05:58:21 | 2011-08-22 |        1811 | You're the best MMCDLXXXIV
#
#  Partition awards_partitions.p20110103 - partition where
#  employee_created_at >= '2011-01-03 00:00:00' AND employee_created_at < '2011-01-10 00:00:00':
#
#  id  |         created_at         | employee_created_at | awarded_on | employee_id |         award_title
#------+----------------------------+---------------------+------------+-------------+------------------------------
#   61 | 2012-03-27 10:38:39.728924 | 2011-01-07 19:18:54 | 2012-03-27 |        2366 | You're the greatest CCCXLIII
#  134 | 2012-03-27 10:38:39.728924 | 2011-01-07 19:33:11 | 2011-05-17 |         810 | You're the best CXXXIV
#  193 | 2012-03-27 10:38:39.728924 | 2011-01-04 06:58:09 | 2011-05-27 |         836 | You're the best CXCIII
#  ...
# 2218 | 2012-03-27 10:38:44.49816  | 2011-01-06 21:05:51 | 2011-07-16 |        2190 | You're the best MMCCXVIII
# 2245 | 2012-03-27 10:38:44.630179 | 2011-01-08 11:44:32 | 2011-04-17 |         841 | You're the best MMCCXLV
# 2499 | 2012-03-27 10:38:45.959831 | 2011-01-07 02:25:00 | 2011-09-07 |        1717 | You're the best MMCDXCIX
#
#  Partition employees_partitions.p20110110 - partition where
#  employee_created_at >= '2011-01-10 00:00:00' AND employee_created_at < '2011-01-17 00:00:00':
#
#  id  |         created_at         | employee_created_at | awarded_on | employee_id |        award_title
#------+----------------------------+---------------------+------------+-------------+---------------------------
#   36 | 2012-03-27 10:38:39.728924 | 2011-01-12 03:12:37 | 2011-06-23 |        1196 | You're the best XXXVI
#   44 | 2012-03-27 10:38:39.728924 | 2011-01-15 20:43:01 | 2011-10-09 |        1159 | You're the best XLIV
#   93 | 2012-03-27 10:38:39.728924 | 2011-01-16 14:45:36 | 2011-10-02 |         912 | You're the best XCIII
#  ...
# 2476 | 2012-03-27 10:38:45.848691 | 2011-01-10 23:23:24 | 2011-11-05 |        1314 | You're the best MMCDLXXVI
# 2481 | 2012-03-27 10:38:45.873229 | 2011-01-11 05:12:47 | 2011-08-10 |        2240 | You're the best MMCDLXXXI
# 2491 | 2012-03-27 10:38:45.921378 | 2011-01-12 04:18:22 | 2011-08-13 |        1595 | You're the best MMCDXCI
#
# ...
#
#  Partition awards_partitions.p20111212 - partition where
#  employee_created_at >= '2011-12-12 00:00:00' AND employee_created_at < '2011-12-19 00:00:00':
#
#  id  |         created_at         | employee_created_at | awarded_on | employee_id |          award_title
#------+----------------------------+---------------------+------------+-------------+--------------------------------
#   76 | 2012-03-27 10:38:39.728924 | 2011-12-17 11:12:41 | 2012-03-27 |        1548 | You're the greatest DXXIII
#  159 | 2012-03-27 10:38:39.728924 | 2011-12-17 17:04:37 | 2012-03-27 |        2351 | You're the greatest DCCCLXXXII
#  181 | 2012-03-27 10:38:39.728924 | 2011-12-18 14:25:29 | 2012-04-20 |        2031 | You're the best CLXXXI
#  ...
# 2197 | 2012-03-27 10:38:44.39818  | 2011-12-13 00:14:00 | 2012-06-11 |         555 | You're the best MMCXCVII
# 2216 | 2012-03-27 10:38:44.488553 | 2011-12-13 17:08:24 | 2011-12-26 |         596 | You're the best MMCCXVI
# 2217 | 2012-03-27 10:38:44.493389 | 2011-12-16 18:12:58 | 2012-12-14 |        1860 | You're the best MMCCXVII
#
#  Partition awards_partitions.p20111219 - partition where
#  employee_created_at >= '2011-12-19 00:00:00' AND employee_created_at < '2011-12-26 00:00:00':
#
#  id  |         created_at         | employee_created_at | awarded_on | employee_id |         award_title
#------+----------------------------+---------------------+------------+-------------+-----------------------------
#   16 | 2012-03-27 10:38:39.728924 | 2011-12-23 17:53:33 | 2012-05-06 |        1534 | You're the best XVI
#  132 | 2012-03-27 10:38:39.728924 | 2011-12-19 21:58:52 | 2012-08-28 |         979 | You're the best CXXXII
#  149 | 2012-03-27 10:38:39.728924 | 2011-12-24 06:28:33 | 2012-04-06 |         507 | You're the best CXLIX
#  ...
# 2451 | 2012-03-27 10:38:45.726195 | 2011-12-23 22:40:28 | 2012-11-05 |         914 | You're the best MMCDLI
# 2483 | 2012-03-27 10:38:45.882936 | 2011-12-20 13:54:57 | 2012-05-09 |        1615 | You're the best MMCDLXXXIII
# 2485 | 2012-03-27 10:38:45.892318 | 2011-12-25 06:54:49 | 2012-03-27 |        2017 | You're the greatest CLIII
#
#  Partition awards_partitions.p20111226 - partition where
#  employee_created_at >= '2011-12-26 00:00:00' AND employee_created_at < '2012-01-02 00:00:00':
#
#  id  |         created_at         | employee_created_at | awarded_on | employee_id |         award_title
#------+----------------------------+---------------------+------------+-------------+-----------------------------
#   15 | 2012-03-27 10:38:39.728924 | 2011-12-27 22:35:27 | 2012-03-27 |        1496 | You're the greatest CXXIII
#   62 | 2012-03-27 10:38:39.728924 | 2011-12-27 18:36:46 | 2012-03-27 |        1663 | You're the greatest CLXXXII
#   64 | 2012-03-27 10:38:39.728924 | 2011-12-26 14:49:13 | 2012-04-02 |        2417 | You're the best LXIV
#  ...
# 2211 | 2012-03-27 10:38:44.464965 | 2011-12-27 10:11:18 | 2012-03-27 |        2388 | You're the greatest XXX
# 2291 | 2012-03-27 10:38:44.908378 | 2011-12-26 08:07:37 | 2012-03-27 |         242 | You're the greatest LXXXVI
# 2497 | 2012-03-27 10:38:45.949948 | 2011-12-27 16:51:56 | 2012-11-10 |        1902 | You're the best MMCDXCVII
#

require File.expand_path(File.dirname(__FILE__) + "/lib/command_line_tool_mixin")
require File.expand_path(File.dirname(__FILE__) + "/lib/get_options")

include CommandLineToolMixin

$cleanup = false
$force = false
$create_many = 1500
$create_individual = 500
$new_individual = 500
$update_many = 500
$update_individual = 500

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
  ActiveRecord::Base.connection.drop_schema("awards_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("awards") rescue nil
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if $cleanup
end

$total_records = ($create_many + $create_individual + $new_individual) * 2

puts "total records: #{$total_records}"

START_DATE = Date.parse('2011-01-01')
END_DATE = Date.parse('2011-12-31')

# the ActiveRecord classes

require File.expand_path(File.dirname(__FILE__) + "/lib/company")

class Employee < Partitioned::ByCreatedAt
  belongs_to :company, :class_name => 'Company'
  attr_accessible :created_at, :salary, :name, :company_id

  partitioned do |partition|
    partition.index :id, :unique => true
    partition.foreign_key :company_id
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

class ByEmployeeCreatedAt < Partitioned::ByWeeklyTimeField
  self.abstract_class = true

  def self.partition_time_field
    return :employee_created_at
  end
end

class Award < ByEmployeeCreatedAt
  belongs_to :employee, :class_name => 'Employee'
  attr_accessible :award_title, :employee_created_at, :employee_id, :awarded_on

  partitioned do |partition|
    partition.foreign_key lambda {|model, *partition_key_values|
      return Configurator::Data::ForeignKey.new(:employee_id, Employee.partition_table_name(*partition_key_values), :id)
    }
  end
  
  connection.execute <<-SQL
    create table awards
    (
        id                    serial not null primary key,
        created_at            timestamp not null default now(),
        employee_created_at   timestamp not null,
        awarded_on            date not null,
        employee_id           integer not null,
        award_title           text not null
    );
  SQL
end

# You should have the following tables:
#  public.companies
#  public.employees

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# create the infrastructure for Awards table which includes the schema and partition tables

Award.create_infrastructure

# You should have the following schema:
#  employees_partitions
#  awards_partitions

dates = Employee.partition_generate_range(START_DATE, END_DATE)

Employee.create_new_partition_tables(dates)

# You should have the following tables with increments of one week:
#  employees_partitions.p20101227
#  employees_partitions.p20110103
#  employees_partitions.p20110110
#  employees_partitions.p20110117
#  employees_partitions.p20110124
#  employees_partitions.p20110131
#  employees_partitions.p20110207
#  employees_partitions.p20110214
#  employees_partitions.p20110221
#  employees_partitions.p20110228
#  employees_partitions.p20110307
#  employees_partitions.p20110314
#  employees_partitions.p20110321
#  employees_partitions.p20110328
#  employees_partitions.p20110404
#  employees_partitions.p20110411
#  employees_partitions.p20110418
#  employees_partitions.p20110425
#  employees_partitions.p20110502
#  employees_partitions.p20110509
#  employees_partitions.p20110516
#  employees_partitions.p20110523
#  employees_partitions.p20110530
#  employees_partitions.p20110606
#  employees_partitions.p20110613
#  employees_partitions.p20110620
#  employees_partitions.p20110627
#  employees_partitions.p20110704
#  employees_partitions.p20110711
#  employees_partitions.p20110718
#  employees_partitions.p20110725
#  employees_partitions.p20110801
#  employees_partitions.p20110808
#  employees_partitions.p20110815
#  employees_partitions.p20110822
#  employees_partitions.p20110829
#  employees_partitions.p20110905
#  employees_partitions.p20110912
#  employees_partitions.p20110919
#  employees_partitions.p20110926
#  employees_partitions.p20111003
#  employees_partitions.p20111010
#  employees_partitions.p20111017
#  employees_partitions.p20111024
#  employees_partitions.p20111031
#  employees_partitions.p20111107
#  employees_partitions.p20111114
#  employees_partitions.p20111121
#  employees_partitions.p20111128
#  employees_partitions.p20111205
#  employees_partitions.p20111212
#  employees_partitions.p20111219
#  employees_partitions.p20111226

Award.create_new_partition_tables(dates)

# You should have the following tables with increments of one week:
#  awards_partitions.p20101227
#  awards_partitions.p20110103
#  awards_partitions.p20110110
#  awards_partitions.p20110117
#  awards_partitions.p20110124
#  awards_partitions.p20110131
#  awards_partitions.p20110207
#  awards_partitions.p20110214
#  awards_partitions.p20110221
#  awards_partitions.p20110228
#  awards_partitions.p20110307
#  awards_partitions.p20110314
#  awards_partitions.p20110321
#  awards_partitions.p20110328
#  awards_partitions.p20110404
#  awards_partitions.p20110411
#  awards_partitions.p20110418
#  awards_partitions.p20110425
#  awards_partitions.p20110502
#  awards_partitions.p20110509
#  awards_partitions.p20110516
#  awards_partitions.p20110523
#  awards_partitions.p20110530
#  awards_partitions.p20110606
#  awards_partitions.p20110613
#  awards_partitions.p20110620
#  awards_partitions.p20110627
#  awards_partitions.p20110704
#  awards_partitions.p20110711
#  awards_partitions.p20110718
#  awards_partitions.p20110725
#  awards_partitions.p20110801
#  awards_partitions.p20110808
#  awards_partitions.p20110815
#  awards_partitions.p20110822
#  awards_partitions.p20110829
#  awards_partitions.p20110905
#  awards_partitions.p20110912
#  awards_partitions.p20110919
#  awards_partitions.p20110926
#  awards_partitions.p20111003
#  awards_partitions.p20111010
#  awards_partitions.p20111017
#  awards_partitions.p20111024
#  awards_partitions.p20111031
#  awards_partitions.p20111107
#  awards_partitions.p20111114
#  awards_partitions.p20111121
#  awards_partitions.p20111128
#  awards_partitions.p20111205
#  awards_partitions.p20111212
#  awards_partitions.p20111219
#  awards_partitions.p20111226

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
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
end

puts "creating many employees #{$create_many}"
Employee.create_many(employees)
base += $create_many

puts "creating individual employees #{$create_individual}"
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

puts "new individual employees #{$new_individual}"
(1..$new_individual).each do |i|
  employee_data = {
    :name => "Picholine Pimplenad, #{to_roman(base+i)}",
    :created_at => START_DATE + rand(END_DATE - START_DATE) + rand(1.day.seconds).seconds,
    :salary => rand(80000) + 60000,
    :company_id => company_ids[rand company_ids.length]
  }
  employee = Employee.new(employee_data)
  employee.save
  employees << employee
end
base += $new_individual

updates = {}
puts "update many employees #{$update_many}"
(1..$update_many).each do |i|
  index = rand(employees.length)
  employee_record = employees[index]
  updates[{
            :id => employee_record[:id],
            :created_at => employee_record[:created_at]
          }] = {
    :salary => 100
  }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual employees #{$update_individual}"
(1..$update_individual).each do |i|
  index = rand(employees.length)
  employee_record = employees[index]
  employee = Employee.from_partition(employee_record[:created_at]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end

# generates data for awards_partitions and awards tables

awards = []

base = 0
(1..$create_many).each do |i|
  employee_record = employees[rand(employees.length)]
  awards << {
    :award_title => "You're the best #{to_roman(base+i)}",
    :awarded_on => employee_record[:created_at] + rand(1.year.seconds).seconds,
    :employee_created_at => employee_record[:created_at],
    :employee_id => employee_record[:id]
  }
end

puts "creating many awards #{$create_many}"
Award.create_many(awards)
base += $create_many

puts "creating individual awards #{$create_individual}"
(1..$create_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  award_data = {
    :award_title => "You're the best #{to_roman(base+i)}",
    :awarded_on => employee_record[:created_at] + rand(1.year.seconds).seconds,
    :employee_created_at => employee_record[:created_at],
    :employee_id => employee_record[:id]
  }
  awards << Award.create(award_data)
end
base += $create_individual

puts "new individual awards #{$new_individual}"
(1..$new_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  award_data = {
    :award_title => "You're the best #{to_roman(base+i)}",
    :awarded_on => employee_record[:created_at] + rand(1.year.seconds).seconds,
    :employee_created_at => employee_record[:created_at],
    :employee_id => employee_record[:id]
  }
  award = Award.new(award_data)
  award.save
  awards << award
end

base = 0
updates = {}
puts "update many awards #{$update_many}"
(1..$update_many).each do |i|
  award_record = awards[rand(awards.length)]
  updates[{
            :id => award_record[:id],
            :employee_created_at => award_record[:employee_created_at]
          }] = {
    :award_title => "You're the greatest #{to_roman(base+i)}"
  }
end

Award.update_many(updates, {:set_array => '"award_title = datatable.award_title, awarded_on = now()"'})
base += $update_many

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  award_record = awards[rand(awards.length)]
  award = Award.from_partition(award_record[:employee_created_at]).find(award_record[:id])
  award.awarded_on = Date.parse(Time.now.to_s)
  award.award_title = "You're the greatest #{to_roman(base+i)}"
  award.save
end
