#!/usr/bin/env ../spec/dummy/script/rails runner
require File.expand_path(File.dirname(__FILE__) + "/lib/command_line_tool_mixin")
require File.expand_path(File.dirname(__FILE__) + "/lib/get_options")

include CommandLineToolMixin

$cleanup = false
$force = false

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
}

command_line_options(@options) do |option,argument|
  if option == '--cleanup'
    $cleanup = true
  elsif option == '--force'
    $force = true
  end
end

if $cleanup || $force
  ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true) rescue nil
  ActiveRecord::Base.connection.drop_table("employees") rescue nil
  ActiveRecord::Base.connection.drop_table("companies") rescue nil
  exit(0) if $cleanup
end

# the ActiveRecord classes
require File.expand_path(File.dirname(__FILE__) + "/lib/company")
require File.expand_path(File.dirname(__FILE__) + "/lib/by_company_id")

class ByCompanyIdWithAutoCreateAndDrop < ByCompanyId
  self.abstract_class = true

  partitioned do |partition|
    partition.janitorial_creates_needed lambda { |model, *partition_key_values|
      Company.select(:id).all.map { |company|
        [*partition_key_values, company.id]
      }.reject { |company_partition_key_values|
        model.partition_exists?(*company_partition_key_values)
      }
    }
    partition.janitorial_drops_needed lambda { |model, *partition_key_values|
      model.child_partition_names(*partition_key_values).map { |child_partition_name|
        model.key_values_from_partition_name(child_partition_name)
      }.reject { |child_partition_key_values|
        Company.where(:id => child_partition_key_values[-1]).count > 0
      }
    }
  end
end

class ByCreatedAtWithAutoCreateAndDrop < Partitioned::ByCreatedAt
  self.abstract_class = true

  def self.future_partition_weeks
    return 12
  end

  def self.expired_partition_weeks
    return 4
  end

  partitioned do |partition|
    # Create future_partition_weeks into the future.
    partition.janitorial_creates_needed lambda { |model, *partition_key_values|
      week_count = future_partition_weeks
      current_time = Date.today.at_beginning_of_week
      weeks = (0..week_count).to_a.map { |increment| current_time + increment.weeks }
      weeks.reject! { |week| model.partition_exists?(*partition_key_values, week) }
      weeks.map { |week| [*partition_key_values, week] }
    }
    
    # Drop any partitions older than expired_partition_weeks
    partition.janitorial_drops_needed lambda { |model, *partition_key_values|
      start_partition_name = model.last_n_child_partition_names(1, *partition_key_values).first
      start_date = model.key_values_from_partition_name(start_partition_name).last
      end_date = expired_partition_weeks.weeks.ago.to_date
      weeks = []
      week = start_date
      while week < end_date
        weeks << week
        week += 1.week
      end
      expired_weeks = weeks.select { |week| model.partition_exists?(*partition_key_values, week) }
      expired_weeks.map { |week| [*partition_key_values, week] }
    }
  end
end

class Employee < Partitioned::MultiLevel
  belongs_to :company, :class_name => 'Company'
  attr_accessible :created_at, :salary, :company_id, :name

  partitioned do |partition|
    partition.using_classes ByCompanyIdWithAutoCreateAndDrop, ByCreatedAtWithAutoCreateAndDrop
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

# create the employees_partitions schema and partitions
# for each company and 12-week range.
Employee.create_infrastructure
Employee.create_new_partitions

# drop a company
Company.first.destroy

# create a weekly partition more than 4 weeks in the past
Employee.create_new_partition(Company.first.id, Date.today.at_beginning_of_week - 5.weeks)

# Drop the partitions corresponding to the defunct company and the old weekly partition.
Employee.drop_old_partitions
