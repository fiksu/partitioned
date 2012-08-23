require 'spec_helper'
require "#{File.dirname(__FILE__)}/support/tables_spec_helper"

module ActiveRecord::ConnectionAdapters

  describe "TableDefinition" do

    describe "check_constraint" do

      it "returns an array of constraints" do
        TableDefinition.new(nil).check_constraint("( id >= 0 and id < 10 )").
            first.to_sql.should == "CHECK (( id >= 0 and id < 10 ))"
      end # returns an array of constraints

    end # check_constraint

  end # TableDefinition

  describe "PostgreSQLAdapter" do

    let(:check_existence_schema) do
      ActiveRecord::Base.connection.execute <<-SQL
        select oid from pg_catalog.pg_namespace where nspname='employees_partitions';
      SQL
    end

    let(:create_new_schema) do
      ActiveRecord::Base.connection.execute <<-SQL
        create schema employees_partitions;
      SQL
    end

    describe "check next_sequence_value and next_sequence_values methods" do

      include TablesSpecHelper

      before do
        class Employee < ActiveRecord::Base
          include Partitioned::ActiveRecordOverrides
          extend Partitioned::BulkMethodsMixin
        end
        create_tables
      end

      describe "next_sequence_value" do

        it "returns next_sequence_value" do
          ActiveRecord::Base.connection.next_sequence_value(Employee.sequence_name).should == 1
          ActiveRecord::Base.connection.execute <<-SQL
            insert into employees(name, company_id) values ('Nikita', 1);
          SQL
          ActiveRecord::Base.connection.next_sequence_value(Employee.sequence_name).should == 3
          ActiveRecord::Base.connection.next_sequence_value(Employee.sequence_name).should == 4
        end

      end # next_sequence_value

      describe "next_sequence_values" do

        it "returns five next_sequence_values" do
          ActiveRecord::Base.connection.next_sequence_values(Employee.sequence_name, 5).should == [1, 2, 3, 4, 5]
        end

      end # next_sequence_values

    end # check next_sequence_value and next_sequence_values methods

    describe "create_schema" do

      context "when call without options" do

        it "created schema" do
          ActiveRecord::Base.connection.create_schema("employees_partitions")
          check_existence_schema.values.should_not be_blank
        end # created schema

      end # when call without options

      context "when call with options unless_exists = true and schema with this name already exist" do

        it "returns nil if schema already exist" do
          create_new_schema
          default_schema = check_existence_schema
          ActiveRecord::Base.connection.create_schema("employees_partitions", :unless_exists => true)
          default_schema.values.should == check_existence_schema.values
        end # returns nil if schema exist

      end # when call with options unless_exists = true and schema with this name already exist

      context "when call with options unless_exists = false and schema with this name already exist" do

        it "raises ActiveRecord::StatementInvalid" do
          create_new_schema
          lambda {
            ActiveRecord::Base.
                connection.create_schema("employees_partitions", :unless_exists => false)
          }.should raise_error(ActiveRecord::StatementInvalid)
        end # raises ActiveRecord::StatementInvalid

      end # when call with options unless_exists = false and schema with this name already exist

    end # create_schema

    describe "drop_schema" do

      context "when call without options" do

        it "deleted schema" do
          create_new_schema
          ActiveRecord::Base.connection.drop_schema("employees_partitions")
          check_existence_schema.values.should be_blank
        end

      end # when call without options

      context "when call with options if_exist = true and schema with this name don't exist" do

        it "deleted schema" do
          ActiveRecord::Base.connection.drop_schema("employees_partitions", :if_exists => true)
          check_existence_schema.values.should be_blank
        end

      end # when call with options if_exist = true and schema with this name don't exist

      context "when call with options if_exist = false and schema with this name don't exist" do

        it "raises ActiveRecord::StatementInvalid" do
          lambda {
            ActiveRecord::Base.
                connection.drop_schema("employees_partitions", :if_exists => false)
          }.should raise_error(ActiveRecord::StatementInvalid)
        end

      end # when call with options if_exist = false and schema with this name don't exist

      context "when call with option cascade = true" do

        it "deleted schema cascade" do
          create_new_schema
          ActiveRecord::Base.connection.execute <<-SQL
            create table employees_partitions.temp();
          SQL
          ActiveRecord::Base.connection.drop_schema("employees_partitions", :cascade => true)
          check_existence_schema.values.should be_blank
        end

      end # when call with option cascade = true

    end # drop_schema

    describe "add_foreign_key" do

      it "added foreign key constraint" do
        create_new_schema
        ActiveRecord::Base.connection.execute <<-SQL
          create table employees_partitions.temp(
            id            serial not null primary key,
            company_id    integer not null
          );
          create table companies(
            id      serial not null primary key
          );
        SQL
        ActiveRecord::Base.connection.add_foreign_key("employees_partitions.temp", :company_id, "companies", :id)
        result = ActiveRecord::Base.connection.execute <<-SQL
          SELECT constraint_type FROM information_schema.table_constraints
          WHERE table_name = 'temp' AND constraint_name = 'temp_company_id_fkey';
        SQL
        result.values.first.should == ["FOREIGN KEY"]
      end

    end # add_foreign_key

  end # PostgreSQLAdapter

end # ActiveRecord::ConnectionAdapters
