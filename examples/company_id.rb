#!/usr/bin/env ../spec/dummy/script/rails runner
# if you use linux, please change previous line to the
# "#! ../spec/dummy/script/rails runner"

# Before running this example you should execute "bundle install" and "rake db:create".
# To run this example you should open 'example' directory and execute example with one of the following flags:
# -C    cleanup data in database and exit;
# -F    cleanup data in database before creating new data;
#
# For example:
# ./company_id.rb - F

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
#  Break criterion is a company (company_id).
#
# Implementation:
#
#  Class Employee inherits from the abstract class ByCompanyId,
#  which supports partitioning.
#
#  class Employee < ByCompanyId
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
#  end
#
#  Create a schema employees_partitions, within which to store all of our partitions:
#
#  Employee.create_infrastructure
#
#  Create a partitions for each company:
#
#  company_ids = Company.all.map(&:id)
#  Employee.create_new_partition_tables(company_ids)
#
#  Each of partition has the same structure as that of the employees table:
#
#   id | created_at | updated_at | name | salary | company_id
#  ----+------------+------------+------+--------+------------
#
#  CREATE TABLE "employees_partitions"."p1" (CHECK (( company_id = 1 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p1_on_id" ON "employees_partitions"."p1" ("id");
#  ALTER TABLE employees_partitions.p1 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p2" (CHECK (( company_id = 2 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p2_on_id" ON "employees_partitions"."p2" ("id");
#  ALTER TABLE employees_partitions.p2 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p3" (CHECK (( company_id = 3 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p3_on_id" ON "employees_partitions"."p3" ("id");
#  ALTER TABLE employees_partitions.p3 add foreign key (company_id) references companies(id);
#
#  CREATE TABLE "employees_partitions"."p4" (CHECK (( company_id = 4 ))) INHERITS (employees);
#  CREATE UNIQUE INDEX "index_employees_partitions.p4_on_id" ON "employees_partitions"."p4" ("id");
#  ALTER TABLE employees_partitions.p4 add foreign key (company_id) references companies(id);
#
#  Since we have done four records of companies in the table,
#  we have four partitions:
#
#  employees_partitions.p1
#  employees_partitions.p2
#  employees_partitions.p3
#  employees_partitions.p4
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
#                                   datatable.salary, updated_at = now()"' })
#
#  This construction using for update one record. You also may use update method.
#  employee = Employee.from_partition(employee_record[:company_id]).find(employee_record[:id])
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
#  id  |         created_at         |         updated_at         |               name                  |   salary    | company_id
#------+----------------------------+----------------------------+-------------------------------------+-------------+------------
#    1 | 2012-03-26 11:26:30.704959 | 2012-03-26 11:26:45.519874 | Winston J. Sillypants, I            | $106,363.00 |     4
#    2 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, II           | $103,767.00 |     3
#    3 | 2012-03-26 11:26:30.704959 | 2012-03-26 11:26:43.250032 | Winston J. Sillypants, III          | $128,998.00 |     1
#  ...
# 4998 | 2012-03-26 11:26:40.43347  | 2012-03-26 11:26:44.570338 | Picholine Pimplenad, MMMMCMXCVIII   |  $93,628.00 |     3
# 4999 | 2012-03-26 11:26:40.437824 | 2012-03-26 11:26:40.437824 | Picholine Pimplenad, MMMMCMXCIX     | $133,964.00 |     3
# 5000 | 2012-03-26 11:26:40.441958 | 2012-03-26 11:26:40.441958 | Picholine Pimplenad, _V             |  $76,519.00 |     4
#
#  Partition employees_partitions.p1 - partition where company_id = 1:
#
#  id  |         created_at         |         updated_at         |                name                 |   salary    | company_id
#------+----------------------------+----------------------------+-------------------------------------+-------------+------------
#    3 | 2012-03-26 11:26:30.704959 | 2012-03-26 11:26:43.250032 | Winston J. Sillypants, III          | $128,998.00 |     1
#    5 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, V            | $134,319.00 |     1
#    8 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, VIII         |  $82,995.00 |     1
#  ...
# 4988 | 2012-03-26 11:26:40.392319 | 2012-03-26 11:26:44.077802 | Picholine Pimplenad, MMMMCMLXXXVIII | $132,535.00 |     1
# 4994 | 2012-03-26 11:26:40.416951 | 2012-03-26 11:26:40.416951 | Picholine Pimplenad, MMMMCMXCIV     | $105,119.00 |     1
# 4996 | 2012-03-26 11:26:40.425268 | 2012-03-26 11:26:40.425268 | Picholine Pimplenad, MMMMCMXCVI     |  $81,403.00 |     1
#
#  Partition employees_partitions.p2 - partition where company_id = 2:
#
#  id  |         created_at         |         updated_at         |              name                   |   salary    | company_id
#------+----------------------------+----------------------------+-------------------------------------+-------------+------------
#    4 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, IV           | $136,540.00 |     2
#   12 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, XII          | $103,200.00 |     2
#   13 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, XIII         | $139,077.00 |     2
#  ...
# 4991 | 2012-03-26 11:26:40.40451  | 2012-03-26 11:26:42.057637 | Picholine Pimplenad, MMMMCMXCI      | $122,115.00 |     2
# 4992 | 2012-03-26 11:26:40.408519 | 2012-03-26 11:26:40.408519 | Picholine Pimplenad, MMMMCMXCII     |  $90,176.00 |     2
# 4995 | 2012-03-26 11:26:40.421126 | 2012-03-26 12:26:49.969993 | Picholine Pimplenad, MMMMCMXCV      |  $86,410.00 |     2
#
#
#  Partition employees_partitions.p3 - partition where company_id = 3:
#
#  id  |         created_at         |         updated_at         |               name                  |   salary    | company_id
#------+----------------------------+----------------------------+-------------------------------------+-------------+------------
#    2 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, II           | $103,767.00 |     3
#    6 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, VI           |  $67,280.00 |     3
#    9 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, IX           |  $75,396.00 |     3
#  ...
# 4990 | 2012-03-26 11:26:40.400746 | 2012-03-26 11:26:43.604349 | Picholine Pimplenad, MMMMCMXC       |  $95,882.00 |     3
# 4998 | 2012-03-26 11:26:40.43347  | 2012-03-26 11:26:44.570338 | Picholine Pimplenad, MMMMCMXCVIII   |  $93,628.00 |     3
# 4999 | 2012-03-26 11:26:40.437824 | 2012-03-26 11:26:40.437824 | Picholine Pimplenad, MMMMCMXCIX     | $133,964.00 |     3
#
#
#  Partition employees_partitions.p4 - partition where company_id = 4:
#
#  id  |         created_at         |         updated_at         |               name                  |   salary    | company_id
#------+----------------------------+----------------------------+-------------------------------------+-------------+------------
#    1 | 2012-03-26 11:26:30.704959 | 2012-03-26 11:26:45.519874 | Winston J. Sillypants, I            | $106,363.00 |     4
#    7 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, VII          | $111,585.00 |     4
#   17 | 2012-03-26 11:26:30.704959 |                            | Winston J. Sillypants, XVII         | $135,812.00 |     4
#  ...
# 4993 | 2012-03-26 11:26:40.412815 | 2012-03-26 11:26:40.412815 | Picholine Pimplenad, MMMMCMXCIII    |  $95,851.00 |     4
# 4997 | 2012-03-26 11:26:40.429356 | 2012-03-26 11:26:40.429356 | Picholine Pimplenad, MMMMCMXCVII    |  $84,564.00 |     4
# 5000 | 2012-03-26 11:26:40.441958 | 2012-03-26 11:26:40.441958 | Picholine Pimplenad, _V             |  $76,519.00 |     4
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
require File.expand_path(File.dirname(__FILE__) + "/lib/by_company_id")

class Employee < ByCompanyId
  belongs_to :company, :class_name => 'Company'
  attr_accessible :salary, :company_id, :name

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
# employees_partitions

# add some companies

Company.create_many(COMPANIES)
company_ids = Company.all.map(&:id)

# create the employees partitions dependant on the all companies

Employee.create_new_partition_tables(company_ids)

# You should have the following tables:
#  employees_partitions.p1
#  employees_partitions.p2
#  employees_partitions.p3
#  employees_partitions.p4

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
  updates[{
            :id => employee_record[:id],
            :company_id => employee_record[:company_id]
          }] = {
      :salary => 100
  }
end

Employee.update_many(updates, {:set_array => '"salary = #{table_name}.salary + datatable.salary, updated_at = now()"'})

puts "update individual #{$update_individual}"
(1..$update_individual).each do |i|
  employee_record = employees[rand(employees.length)]
  employee = Employee.from_partition(employee_record[:company_id]).find(employee_record[:id])
  employee.salary += 1000
  employee.save
end

