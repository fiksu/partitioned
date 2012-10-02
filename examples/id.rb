#!/usr/bin/env ../spec/dummy/script/rails runner
# if you use linux, please change previous line to the
# "#! ../spec/dummy/script/rails runner"

# Before running this example you should execute "bundle install" and "rake db:create".
# To run this example you should open 'example' directory and execute example with one of the following flags:
# -C    cleanup data in database and exit;
# -F    cleanup data in database before creating new data;
#
# For example:
# ./id.rb - F

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
#  Break criterion is an id.
#
# Implementation:
#
#  Class Employee inherits from the abstract class ById,
#  which supports partitioning.
#
#  class Employee < Partitioned::ById
#
#    Indicates a relationship to the companies table.
#    belongs_to :company, :class_name => 'Company'
#
#    Partition table size defines a count of records in one partition table.
#    def self.partition_table_size
#      return 10
#    end
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
#       partition.foreign_key :company_id
#    end
#  end
#
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partition tables with increments of 10 records:
#
#  ids = Employee.partition_generate_range(0, NUM_EMPLOYEES, Employee.partition_table_size)
#  Employee.create_new_partition_tables(ids)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p0" (CHECK (( id >= 0 and id < 10 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p0_id_udx" ON "employees_partitions"."p0" ("id");
#  ALTER TABLE employees_partitions.p0 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p10" (CHECK (( id >= 10 and id < 20 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p10_id_udx" ON "employees_partitions"."p10" ("id");
#  ALTER TABLE employees_partitions.p10 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p20" (CHECK (( id >= 20 and id < 30 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p20_id_udx" ON "employees_partitions"."p20" ("id");
#  ALTER TABLE employees_partitions.p20 add foreign key (company_id) references companies(id);
#
#  ...
#  CREATE TABLE "employees_partitions"."p4980" (CHECK (( id >= 4980 and id < 4990 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p4980_id_udx" ON "employees_partitions"."p4980" ("id");
#  ALTER TABLE employees_partitions.p4980 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p4990" (CHECK (( id >= 4990 and id < 5000 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p4990_id_udx" ON "employees_partitions"."p4990" ("id");
#  ALTER TABLE employees_partitions.p4990 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p5000" (CHECK (( id >= 5000 and id < 5010 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "p5000_id_udx" ON "employees_partitions"."p5000" ("id");
#  ALTER TABLE employees_partitions.p5000 add foreign key (company_id) references companies(id);
#
#  You should have the following tables with increments of 10 ids:
#  employees_partitions.p0
#  employees_partitions.p10
#  employees_partitions.p20
#  ...
#  employees_partitions.p4980
#  employees_partitions.p4990
#  employees_partitions.p5000
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
#                                         datatable.salary, updated_at = now()"'})
#
#  This construction using for update one record. You also may use update method.
#  employee = Employee.from_partition(employee_record[:id]).find(employee_record[:id])
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
#  id  |         created_at         |         updated_at         |               name                |   salary    | company_id
#------+----------------------------+----------------------------+-----------------------------------+-------------+------------
#    1 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, I          | $128,739.00 |    2
#    2 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, II         | $128,107.00 |    3
#    3 | 2012-03-26 13:24:42.368938 | 2012-03-26 14:25:05.36247  | Winston J. Sillypants, III        |  $64,773.00 |    3
#  ...
# 4998 | 2012-03-26 13:24:54.852909 | 2012-03-26 14:25:04.65906  | Picholine Pimplenad, MMMMCMXCVIII |  $77,423.00 |    2
# 4999 | 2012-03-26 13:24:54.857871 | 2012-03-26 13:24:54.857871 | Picholine Pimplenad, MMMMCMXCIX   | $134,121.00 |    4
# 5000 | 2012-03-26 13:24:54.862685 | 2012-03-26 13:24:54.862685 | Picholine Pimplenad, _V           | $114,432.00 |    1
#
#  Partition employees_partitions.p0 - partition where (id >= 0 AND id < 10):
#
#   id |         created_at         |        updated_at         |            name             |   salary    | company_id
#  ----+----------------------------+---------------------------+-----------------------------+-------------+------------
#    1 | 2012-03-26 13:24:42.368938 |                           | Winston J. Sillypants, I    | $128,739.00 |     2
#    2 | 2012-03-26 13:24:42.368938 |                           | Winston J. Sillypants, II   | $128,107.00 |     3
#    3 | 2012-03-26 13:24:42.368938 | 2012-03-26 14:25:05.36247 | Winston J. Sillypants, III  |  $64,773.00 |     3
#    4 | 2012-03-26 13:24:42.368938 |                           | Winston J. Sillypants, IV   | $138,131.00 |     4
#    5 | 2012-03-26 13:24:42.368938 |                           | Winston J. Sillypants, V    |  $81,823.00 |     3
#    6 | 2012-03-26 13:24:42.368938 |                           | Winston J. Sillypants, VI   |  $66,892.00 |     1
#    7 | 2012-03-26 13:24:42.368938 |                           | Winston J. Sillypants, VII  |  $68,906.00 |     2
#    8 | 2012-03-26 13:24:42.368938 |                           | Winston J. Sillypants, VIII | $116,640.00 |     1
#    9 | 2012-03-26 13:24:42.368938 | 2012-03-26 14:25:05.36247 | Winston J. Sillypants, IX   |  $99,194.00 |     2
#
#  Partition employees_partitions.p10 - partition where (id >= 10 AND id < 20):
#
#   id |         created_at         |         updated_at         |             name             |   salary    | company_id
#  ----+----------------------------+----------------------------+------------------------------+-------------+------------
#   10 | 2012-03-26 13:24:42.368938 | 2012-03-26 13:25:00.75383  | Winston J. Sillypants, X     | $120,320.00 |    4
#   11 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XI    |  $90,422.00 |    4
#   12 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XII   |  $75,570.00 |    4
#   13 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XIII  | $109,886.00 |    1
#   14 | 2012-03-26 13:24:42.368938 | 2012-03-26 13:25:01.58278  | Winston J. Sillypants, XIV   |  $89,954.00 |    3
#   15 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XV    |  $86,063.00 |    2
#   16 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XVI   |  $66,072.00 |    1
#   17 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XVII  |  $84,231.00 |    1
#   18 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XVIII | $105,545.00 |    1
#   19 | 2012-03-26 13:24:42.368938 | 2012-03-26 13:24:59.727553 | Winston J. Sillypants, XIX   | $125,657.00 |    4
#
#  Partition employees_partitions.p20 - partition where (id >= 20 AND id < 30):
#
#   id |         created_at         |         updated_at         |             name              |   salary    | company_id
#  ----+----------------------------+----------------------------+-------------------------------+-------------+------------
#   20 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XX     |  $89,345.00 |    2
#   21 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XXI    |  $92,289.00 |    4
#   22 | 2012-03-26 13:24:42.368938 | 2012-03-26 13:24:58.306639 | Winston J. Sillypants, XXII   |  $97,416.00 |    4
#   23 | 2012-03-26 13:24:42.368938 | 2012-03-26 14:25:05.152222 | Winston J. Sillypants, XXIII  |  $74,307.00 |    1
#   24 | 2012-03-26 13:24:42.368938 | 2012-03-26 14:25:05.152222 | Winston J. Sillypants, XXIV   | $136,770.00 |    4
#   25 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XXV    | $108,876.00 |    1
#   26 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XXVI   |  $84,157.00 |    1
#   27 | 2012-03-26 13:24:42.368938 |                            | Winston J. Sillypants, XXVII  | $108,896.00 |    3
#   28 | 2012-03-26 13:24:42.368938 | 2012-03-26 13:24:56.590543 | Winston J. Sillypants, XXVIII |  $93,987.00 |    2
#   29 | 2012-03-26 13:24:42.368938 | 2012-03-26 14:25:05.152222 | Winston J. Sillypants, XXIX   |  $88,377.00 |    4
#
# ...
#  Partition employees_partitions.p4980 - partition where (id >= 4980 AND id < 4990):
#
#  id  |         created_at         |         updated_at         |                name                 |   salary    | company_id
#------+----------------------------+----------------------------+-------------------------------------+-------------+------------
# 4980 | 2012-03-26 13:24:54.761377 | 2012-03-26 14:25:04.733069 | Picholine Pimplenad, MMMMCMLXXX     | $104,522.00 |    3
# 4981 | 2012-03-26 13:24:54.769005 | 2012-03-26 13:25:02.598973 | Picholine Pimplenad, MMMMCMLXXXI    | $112,196.00 |    2
# 4982 | 2012-03-26 13:24:54.773971 | 2012-03-26 13:24:54.773971 | Picholine Pimplenad, MMMMCMLXXXII   |  $93,853.00 |    4
# 4983 | 2012-03-26 13:24:54.779096 | 2012-03-26 13:24:54.779096 | Picholine Pimplenad, MMMMCMLXXXIII  | $126,166.00 |    1
# 4984 | 2012-03-26 13:24:54.783103 | 2012-03-26 14:25:04.733069 | Picholine Pimplenad, MMMMCMLXXXIV   | $103,503.00 |    4
# 4985 | 2012-03-26 13:24:54.787865 | 2012-03-26 13:24:54.787865 | Picholine Pimplenad, MMMMCMLXXXV    | $115,251.00 |    4
# 4986 | 2012-03-26 13:24:54.792864 | 2012-03-26 13:24:57.905747 | Picholine Pimplenad, MMMMCMLXXXVI   |  $72,873.00 |    4
# 4987 | 2012-03-26 13:24:54.798004 | 2012-03-26 13:24:54.798004 | Picholine Pimplenad, MMMMCMLXXXVII  | $117,931.00 |    4
# 4988 | 2012-03-26 13:24:54.802963 | 2012-03-26 13:24:54.802963 | Picholine Pimplenad, MMMMCMLXXXVIII |  $87,801.00 |    2
# 4989 | 2012-03-26 13:24:54.8078   | 2012-03-26 13:24:54.8078   | Picholine Pimplenad, MMMMCMLXXXIX   |  $99,801.00 |    3
#
#  Partition employees_partitions.p4990 - partition where (id >= 4990 AND id < 5000):
#
#  id  |         created_at         |         updated_at         |               name                |   salary    | company_id
#------+----------------------------+----------------------------+-----------------------------------+-------------+------------
# 4990 | 2012-03-26 13:24:54.812753 | 2012-03-26 13:24:54.812753 | Picholine Pimplenad, MMMMCMXC     |  $69,725.00 |      3
# 4991 | 2012-03-26 13:24:54.820171 | 2012-03-26 13:24:57.663318 | Picholine Pimplenad, MMMMCMXCI    | $128,414.00 |      1
# 4992 | 2012-03-26 13:24:54.825059 | 2012-03-26 13:24:54.825059 | Picholine Pimplenad, MMMMCMXCII   | $113,357.00 |      2
# 4993 | 2012-03-26 13:24:54.829422 | 2012-03-26 13:24:54.829422 | Picholine Pimplenad, MMMMCMXCIII  |  $96,098.00 |      2
# 4994 | 2012-03-26 13:24:54.833662 | 2012-03-26 13:24:54.833662 | Picholine Pimplenad, MMMMCMXCIV   |  $89,013.00 |      2
# 4995 | 2012-03-26 13:24:54.838481 | 2012-03-26 14:25:04.65906  | Picholine Pimplenad, MMMMCMXCV    |  $95,160.00 |      4
# 4996 | 2012-03-26 13:24:54.843238 | 2012-03-26 13:24:54.843238 | Picholine Pimplenad, MMMMCMXCVI   |  $95,245.00 |      3
# 4997 | 2012-03-26 13:24:54.84813  | 2012-03-26 13:24:54.84813  | Picholine Pimplenad, MMMMCMXCVII  | $136,887.00 |      3
# 4998 | 2012-03-26 13:24:54.852909 | 2012-03-26 14:25:04.65906  | Picholine Pimplenad, MMMMCMXCVIII |  $77,423.00 |      2
# 4999 | 2012-03-26 13:24:54.857871 | 2012-03-26 13:24:54.857871 | Picholine Pimplenad, MMMMCMXCIX   | $134,121.00 |      4
#
#  Partition employees_partitions.p5000 - partition where (id >= 5000 AND id < 5010):
#
#  id  |         created_at         |         updated_at         |          name                     |   salary    | company_id
#------+----------------------------+----------------------------+-----------------------------------+-------------+------------
# 5000 | 2012-03-26 13:24:54.862685 | 2012-03-26 13:24:54.862685 | Picholine Pimplenad, _V           | $114,432.00 |      1
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
$partition_table_size = 10

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

# the ActiveRecord classes

require File.expand_path(File.dirname(__FILE__) + "/lib/company")

class Employee < Partitioned::ById
  belongs_to :company, :class_name => 'Company'
  attr_accessible :company_id, :salary, :name

  def self.partition_table_size
    return $partition_table_size
  end

  partitioned do |partition|
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

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

# create the infrastructure for EMPLOYEES table which includes the schema and partition tables

Employee.create_infrastructure

# You should have the following schema:
#  employees_partitions

# Create a partition tables with increments of 10 records, because
# Employee.partition_table_size returns 10
ids = Employee.partition_generate_range(0, $total_records, $partition_table_size)
Employee.create_new_partition_tables(ids)

# You should have the following tables:
#  employees_partitions.p0
#  employees_partitions.p10
#  employees_partitions.p20
#  ...
#  employees_partitions.p4980
#  employees_partitions.p4990
#  employees_partitions.p5000

# now add some employees across the year.

employees = []

require File.expand_path(File.dirname(__FILE__) + "/lib/roman")

# generates data for employees_partitions and employees tables

base = 0
(1..$create_many).each do |i|
  employees << {
    :name => "Winston J. Sillypants, #{to_roman(base+i)}",
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
  updates[{ :id => employee_record[:id]}] = { :salary => 100 }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  employee = Employee.from_partition(employee_record[:id]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end
