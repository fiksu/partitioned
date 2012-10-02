#!/usr/bin/env ../spec/dummy/script/rails runner
# if you use linux, please change previous line to the
# "#! ../spec/dummy/script/rails runner"

# Before running this example you should execute "bundle install" and "rake db:create".
# To run this example you should open 'example' directory and execute example with one of the following flags:
# -C    cleanup data in database and exit;
# -F    cleanup data in database before creating new data;
#
# For example:
# ./created_at.rb - F

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
#  Break criterion is a created date(created_at).
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
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partition tables with increments of one week:
#
#  dates = Employee.partition_generate_range(START_DATE, END_DATE)
#  Employee.create_new_partition_tables(dates)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p20101227" (CHECK (created_at >= '2010-12-27'
#                                AND created_at < '2011-01-03')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20101227_on_created_at"
#                                ON "employees_partitions"."p20101227" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20101227_on_id"
#                                ON "employees_partitions"."p20101227" ("id");
#  ALTER TABLE employees_partitions.p20101227 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20110103" (CHECK (created_at >= '2011-01-03'
#                                AND created_at < '2011-01-10')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20110103_on_created_at"
#                                ON "employees_partitions"."p20110103" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20110103_on_id"
#                                ON "employees_partitions"."p20110103" ("id");
#  ALTER TABLE employees_partitions.p20110103 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20110110" (CHECK (created_at >= '2011-01-10'
#                                AND created_at < '2011-01-17')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20110110_on_created_at"
#                                ON "employees_partitions"."p20110110" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20110110_on_id"
#                                ON "employees_partitions"."p20110110" ("id");
#  ALTER TABLE employees_partitions.p20110110 add foreign key (company_id) references companies(id);
#
#  ...
#  CREATE TABLE "employees_partitions"."p20111212" (CHECK (created_at >= '2011-12-12'
#                               AND created_at < '2011-12-19')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20111212_on_created_at"
#                               ON "employees_partitions"."p20111212" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20111212_on_id"
#                               ON "employees_partitions"."p20111212" ("id");
#  ALTER TABLE employees_partitions.p20111212 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20111219" (CHECK (created_at >= '2011-12-19'
#                               AND created_at < '2011-12-26')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20111219_on_created_at"
#                               ON "employees_partitions"."p20111219" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20111219_on_id"
#                               ON "employees_partitions"."p20111219" ("id");
#  ALTER TABLE employees_partitions.p20111219 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20111226" (CHECK (created_at >= '2011-12-26'
#                               AND created_at < '2012-01-02')) INHERITS (employees);
#  CREATE INDEX "index_employees_partitions.p20111226_on_created_at"
#                               ON "employees_partitions"."p20111226" ("created_at");
#  CREATE UNIQUE INDEX "index_employees_partitions.p20111226_on_id"
#                               ON "employees_partitions"."p20111226" ("id");
#  ALTER TABLE employees_partitions.p20111226 add foreign key (company_id) references companies(id);
#
#  You should have the following tables with increments of one week:
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
#  Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary +
#                                        datatable.salary, updated_at = now()"'})
#
#  This construction using for update one record. You also may use update method.
#  employee = Employee.from_partition(employee_record[:created_at]).find(employee_record[:id])
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
#  Partition employees_partitions.p20101227 - partition where
#  created_at >= '2010-12-27 00:00:00' AND created_at < '2011-01-03 00:00:00':
#
#  id  |     created_at      |         updated_at         |              name              |   salary    | company_id
#------+---------------------+----------------------------+--------------------------------+-------------+------------
#  501 | 2011-01-01 07:19:27 |                            | Winston J. Sillypants, DI      | $114,587.00 |     2
# 1019 | 2011-01-01 15:35:56 | 2012-03-26 13:06:30.045207 | Winston J. Sillypants, MXIX    |  $84,563.00 |     3
# 1093 | 2011-01-02 16:59:03 |                            | Winston J. Sillypants, MXCIII  |  $81,852.00 |     1
#  ...
# 4461 | 2011-01-02 09:10:12 | 2012-03-26 14:06:37.462631 | Picholine Pimplenad, MMMMCDLXI | $115,618.00 |     4
# 4544 | 2011-01-01 09:56:06 | 2012-03-26 13:06:24.708909 | Picholine Pimplenad, MMMMDXLIV |  $71,629.00 |     4
# 4596 | 2011-01-02 03:10:12 | 2012-03-26 13:06:25.027589 | Picholine Pimplenad, MMMMDXCVI |  $88,167.00 |     3
#
#  Partition employees_partitions.p20110103 - partition where
#  created_at >= '2011-01-03 00:00:00' AND created_at < '2011-01-10 00:00:00':
#
#  id  |     created_at      |         updated_at         |                name                |   salary    | company_id
#------+---------------------+----------------------------+------------------------------------+-------------+------------
#   41 | 2011-01-09 04:19:34 |                            | Winston J. Sillypants, XLI         | $125,885.00 |     2
#   68 | 2011-01-05 13:27:00 | 2012-03-26 13:06:31.066556 | Winston J. Sillypants, LXVIII      | $117,202.00 |     2
#   77 | 2011-01-08 11:34:15 |                            | Winston J. Sillypants, LXXVII      |  $68,907.00 |     4
#  ...
# 4858 | 2011-01-03 04:13:29 | 2012-03-26 13:06:26.497023 | Picholine Pimplenad, MMMMDCCCLVIII |  $85,379.00 |     4
# 4865 | 2011-01-04 05:42:59 | 2012-03-26 13:06:26.531102 | Picholine Pimplenad, MMMMDCCCLXV   |  $78,517.00 |     4
# 4943 | 2011-01-05 04:15:05 | 2012-03-26 13:06:26.892893 | Picholine Pimplenad, MMMMCMXLIII   | $137,396.00 |     1
#
#  Partition employees_partitions.p20110110 - partition where
#  created_at >= '2011-01-10 00:00:00' AND created_at < '2011-01-17 00:00:00':
#
#  id  |     created_at      |         updated_at         |              name               |   salary    | company_id
#------+---------------------+----------------------------+---------------------------------+-------------+------------
#    7 | 2011-01-13 04:29:44 |                            | Winston J. Sillypants, VII      |  $65,096.00 |     2
#    8 | 2011-01-14 16:34:09 | 2012-03-26 13:06:30.408574 | Winston J. Sillypants, VIII     |  $96,136.00 |     4
#   53 | 2011-01-14 15:02:05 | 2012-03-26 13:06:30.596073 | Winston J. Sillypants, LIII     | $107,568.00 |     2
#  ...
# 4909 | 2011-01-16 21:35:17 | 2012-03-26 13:06:26.734996 | Picholine Pimplenad, MMMMCMIX   | $117,583.00 |     3
# 4945 | 2011-01-14 13:08:14 | 2012-03-26 13:06:26.902338 | Picholine Pimplenad, MMMMCMXLV  | $138,966.00 |     1
# 4946 | 2011-01-15 13:01:35 | 2012-03-26 14:06:37.290964 | Picholine Pimplenad, MMMMCMXLVI | $131,803.00 |     3
#
# ...
#
#  Partition employees_partitions.p20111212 - partition where
#  created_at >= '2011-12-12 00:00:00' AND created_at < '2011-12-19 00:00:00':
#
#  id  |     created_at      |         updated_at         |                name                |   salary    | company_id
#------+---------------------+----------------------------+------------------------------------+-------------+------------
#   10 | 2011-12-17 11:21:01 |                            | Winston J. Sillypants, X           |  $66,153.00 |     4
#  137 | 2011-12-18 06:55:52 |                            | Winston J. Sillypants, CXXXVII     |  $67,900.00 |     1
#  195 | 2011-12-17 15:39:41 |                            | Winston J. Sillypants, CXCV        | $121,419.00 |     1
#  ...
# 4958 | 2011-12-14 07:16:08 | 2012-03-26 13:06:26.965279 | Picholine Pimplenad, MMMMCMLVIII   | $114,922.00 |     1
# 4987 | 2011-12-12 07:11:57 | 2012-03-26 13:06:30.022127 | Picholine Pimplenad, MMMMCMLXXXVII |  $86,493.00 |     2
# 5000 | 2011-12-18 04:23:49 | 2012-03-26 14:06:37.20847  | Picholine Pimplenad, _V            |  $84,113.00 |     4
#
#  Partition employees_partitions.p20111219 - partition where
#  created_at >= '2011-12-19 00:00:00' AND created_at < '2011-12-26 00:00:00':
#
#  id  |     created_at      |         updated_at         |                name                 |   salary    | company_id
#------+---------------------+----------------------------+-------------------------------------+-------------+------------
#    2 | 2011-12-25 15:24:20 | 2012-03-26 13:06:28.669243 | Winston J. Sillypants, II           |  $67,221.00 |    3
#   73 | 2011-12-20 01:04:46 |                            | Winston J. Sillypants, LXXIII       | $108,260.00 |    4
#   88 | 2011-12-24 17:58:28 |                            | Winston J. Sillypants, LXXXVIII     |  $66,455.00 |    2
#  ...
# 4898 | 2011-12-25 23:23:17 | 2012-03-26 13:06:26.685775 | Picholine Pimplenad, MMMMDCCCXCVIII | $139,737.00 |    3
# 4919 | 2011-12-24 07:30:13 | 2012-03-26 13:06:26.777466 | Picholine Pimplenad, MMMMCMXIX      |  $78,781.00 |    4
# 4988 | 2011-12-20 12:03:47 | 2012-03-26 13:06:27.112457 | Picholine Pimplenad, MMMMCMLXXXVIII | $101,671.00 |    1
#
#  Partition employees_partitions.p20111226 - partition where
#  created_at >= '2011-12-26 00:00:00' AND created_at < '2012-01-02 00:00:00':
#
#  id  |     created_at      |         updated_at         |                name                 |   salary    | company_id
#------+---------------------+----------------------------+-------------------------------------+-------------+------------
#   91 | 2011-12-28 09:57:36 | 2012-03-26 14:06:37.434407 | Winston J. Sillypants, XCI          |  $63,230.00 |    2
#  118 | 2011-12-29 21:05:05 |                            | Winston J. Sillypants, CXVIII       | $131,837.00 |    3
#  146 | 2011-12-27 04:50:46 |                            | Winston J. Sillypants, CXLVI        | $107,580.00 |    1
#  ...
# 4488 | 2011-12-26 03:03:13 | 2012-03-26 13:06:24.43532  | Picholine Pimplenad, MMMMCDLXXXVIII |  $91,115.00 |    3
# 4730 | 2011-12-26 11:07:25 | 2012-03-26 13:06:25.785818 | Picholine Pimplenad, MMMMDCCXXX     |  $95,658.00 |    2
# 4817 | 2011-12-27 12:00:07 | 2012-03-26 13:06:34.28227  | Picholine Pimplenad, MMMMDCCCXVII   |  $63,418.00 |    1

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

class Employee < Partitioned::ByCreatedAt
  belongs_to :company, :class_name => 'Company'
  attr_accessible :name, :company_id, :salary, :created_at

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

# You should have the following tables:
#  public.companies
#  public.employees

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# You should have the following schema:
#  employees_partitions

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

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

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
            :created_at => employee_record[:created_at]
          }] = {
    :salary => 100
  }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  index = rand(employees.length)
  employee_record = employees[index]
  employee = Employee.from_partition(employee_record[:created_at]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end
