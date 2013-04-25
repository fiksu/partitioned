# Partitioned

Partitioned adds assistance to ActiveRecord for manipulating (reading,
creating, updating) an activerecord model that represents data that
may be in one of many database tables (determined by the Models data).

It also has features that support the creation and deleting of child
tables and partitioning support infrastructure.

It supports Postgres partitioning and has specific features to
overcome basic failings of Postgres's implementation of partitioning.

Basics:
A parent table can be inherited by many child tables that inherit most
of the attributes of the parent table including its columns.  child
tables typically (and for the uses of this plugin must) have a unique
check constraint the defines which data should be located in that
specific child table.

Such a constraint allows for the SQL planner to ignore most child
tables and target the (hopefully) one child table that contains the
records interested.  This splits data, and meta-data (indexes) which
provides streamlined targeted access to the desired data.

Support for bulk inserts and bulk updates is also provided via
Partitioned::Base.create_many and Partitioned::Base.update_many.

## Example


Given the following models:

```ruby
class Company < ActiveRecord::Base
end

class ByCompanyId < Partitioned::ByForeignKey
  self.abstract_class = true

  belongs_to :company

  def self.partition_foreign_key
    return :company_id
  end

  partitioned do |partition|
    partition.index :id, :unique => true
  end
end

class Employee < ByCompanyId
end
```

and the following tables:

```sql
-- this is the referenced table
create table companies
(
    id               serial not null primary key,
    created_at       timestamp not null default now(),
    updated_at       timestamp,
    name             text null
);

-- add some companies
insert into table companies (name) values
  ('company 1'),('company 2'),('company 2');

-- this is the parent table
create table employees
(
    id               serial not null primary key,
    created_at       timestamp not null default now(),
    updated_at       timestamp,
    name             text null,
    company_id       integer not null references companies
);
```

We now need to create some infrastructure for partitioned tables,
in particular, we create a schema to hold the child partition
tables of employees.

```ruby
Employee.create_infrastructure
```

Which creates the employees_partitions schema using the following SQL:

```sql
create schema employees_partitions;
```

NOTE: We also install protections on the employees table so it isn't
used as a data table (this SQL is not presented for simplicity but is
apart of the create_infrastructure call).

You can create migration for this Employee in this case:

```ruby
class CreatePartitionEmployee < ActiveRecord::Migration
  def up
    Employee.create_infrastructure
  end

  def down
    Employee.delete_infrastructure
  end
end
```

To add child tables we use the create_new_partitions_tables method:

```ruby
company_ids = Company.all.map(&:id)
Employee.create_new_partition_tables(company_ids)
```

which results in the following SQL:

```sql
create table employees_partitions.p1
  ( CHECK ( company_id = 1 ) ) INHERITS (employees);
create table employees_partitions.p2
  ( CHECK ( company_id = 2 ) ) INHERITS (employees);
create table employees_partitions.p3
  ( CHECK ( company_id = 3 ) ) INHERITS (employees);
```

NOTE: Some other SQL is generated in the above example, specifically
the reference to the companies table needs to be explicitly created
for postgres child tables AND the unique index on 'id' is created.
These are not shown for simplicity.

Now we can do operations involving the child partitions.

Since database records exist in a specific child table dependant on
the field "company_id" we need to have creates that turn into database
inserts of the EMPLOYEES table redirect the record insert into the
specific child table determined by the value of COMPANY_ID

eg:

```ruby
employee = Employee.create(:name => 'Keith', :company_id => 1)
```

this would normally produce the following:

```sql
INSERT INTO employees ('name', company_id) values ('Keith', 1);
```

but with Partitioned we see:

```sql
INSERT INTO employees_partitions.p1 ('name', company_id) values ('Keith', 1);
```

reads of such a table need some assistance to find the specific child
table the record exists in.

Since we are partitioned by company_id the programmer needs to provide
that information when fetching data, or the database will need to
search all child table for the specific record we are looking for.

This is no longer valid (well, doesn't perform well):

```ruby
employee = Employee.find(1)
```

instead, do one of the following:

```ruby
employee = Employee.from_partition(1).find(1)
employee = Employee.find(:first,
                         :conditions => {:name => 'Keith', :company_id => 1})
employee = Employee.find(:first,
                         :conditions => {:id => 1, :company_id => 1})
```

an update (employee.save where the record already exists in the
database) will take advantage of knowing which child table the record
exists in so it can do some optimization.

so, the following works as expected:

```ruby
employee.name = "Not Keith"
employee.save
```

turns into the following SQL:

```sql
update employees_partitions.p1 set name = 'Not Keith' where id = 1;
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

Copyright 2010-2012 fiksu.com, inc, all rights reserved
