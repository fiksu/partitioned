require 'spec_helper'

describe Partitioned::MultiLevel::Configurator::Dsl do

  before(:all) do
    class Employee

    end
  end

  after(:all) do
    Object.send(:remove_const, :Employee)
  end

  let!(:dsl) { Partitioned::MultiLevel::Configurator::Dsl.new(Employee) }

  describe "initialize" do

    let!(:data_stubs) do
      {
        "on_field" => nil,
        "indexes" => [],
        "janitorial_archives_needed" => nil,
        "janitorial_creates_needed" => nil,
        "janitorial_drops_needed" => nil,
        "foreign_keys" => [],
        "last_partitions_order_by_clause" => nil,
        "schema_name" => nil,
        "table_alias_name" => nil,
        "name_prefix" => nil,
        "base_name" => nil,
        "part_name" => nil,
        "table_name" => nil,
        "parent_table_schema_name" => nil,
        "parent_table_name" => nil,
        "check_constraint" => nil,
        "encoded_name" => nil,
        "using_classes" => []
      }
    end

    context "when try to create the new object" do

      context "check the model name" do

        it "returns Employer" do
          dsl.model.should == Employee
        end

      end # check the model name

      context "check the object data" do

        it "returns data" do
          dsl.data.instance_values.should == data_stubs
        end

      end # check the object data

    end # when try to create a new object

  end # initialize

  describe "on" do

    context "when try to set the field which used to partition child tables" do

      it "raises InvalidForMultiLevelPartitioning" do
        lambda {
          dsl.on
        }.should raise_error(Partitioned::MultiLevel::Configurator::Dsl::InvalidForMultiLevelPartitioning)
      end

    end # when try to set the field which used to partition child tables

  end # on

  describe "using_classes" do

    context "when try to set the using_classes field" do

      it "returns using_classes" do
        dsl.using_classes("ByCompanyId", "ByCreatedAt")
        dsl.data.using_classes.first.should == "ByCompanyId"
        dsl.data.using_classes.last.should == "ByCreatedAt"
      end

    end # when try to set the using_classes field

  end # using_classes

end