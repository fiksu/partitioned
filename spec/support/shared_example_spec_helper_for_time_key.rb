DATE_NOW = Date.today

shared_examples_for "check that basic operations with postgres works correctly for time key" do |class_name|

  let!(:subject) do
    class_name.reset_column_information
    class_name
  end

  context "when try to create one record" do

    it "record created" do
      lambda { subject.create(:name => 'Phil', :company_id => 3, :created_at => DATE_NOW + 1)
      }.should_not raise_error
    end

  end # when try to create one record

  context "when try to create one record using new/save" do

    it "record created" do
      lambda {
        instance = subject.new(:name => 'Mike', :company_id => 1, :created_at => DATE_NOW + 1)
        instance.save!
      }.should_not raise_error
    end

  end # when try to create one record using new/save

  context "when try to create many records" do

    it "records created" do
      lambda { subject.create_many([
                                     { :name => 'Alex', :company_id => 2, :created_at => DATE_NOW + 1 },
                                     { :name => 'Aaron', :company_id => 3, :created_at => DATE_NOW + 1 }])
      }.should_not raise_error
    end

  end # when try to create many records

  context "when try to find a record with the search term is id" do

    it "returns employee name" do
      subject.find(1).name.should == "Keith"
    end

  end # when try to find a record with the search term is id

  context "when try to find a record with the search term is name" do

    it "returns employee name" do
      subject.where(:name => 'Keith').first.name.should == "Keith"
    end

  end # when try to find a record with the search term is name

  context "when try to find a record with the search term is company_id" do

    it "returns employee name" do
      subject.where(:company_id => 1).first.name.should == "Keith"
    end

  end # when try to find a record with the search term is company_id

  context "when try to find a record which is showing partition table" do

    it "returns employee name" do
      subject.from_partition(DATE_NOW).find(1).name.should == "Keith"
    end

  end # when try to find a record which is showing partition table

  context "when try to update a record with id = 1" do

    it "returns updated employee name" do
      subject.update(1, :name => 'Kevin')
      subject.find(1).name.should == "Kevin"
    end

  end # when try to update a record with id = 1

  context "when try to update a record with update_many functions" do

    it "returns updated employee name" do
      subject.update_many( {
        { :id => 1 } => {
            :name => 'Alex',
            :company_id => 3,
            :created_at => DATE_NOW
          }
      } )
      subject.find(1).name.should == "Alex"
    end

    it "returns updated employee name" do
      rows = [{
         :id => 1,
         :name => 'Pit',
         :created_at => DATE_NOW
      }]

      options = {
        :set_array => '"name = datatable.name"',
        :where => '"#{table_name}.id = datatable.id"'
      }
      subject.update_many(rows, options)
      subject.find(1).name.should == "Pit"
    end

  end # when try to update a record with update_many functions

  context "when try to delete a record with id = 1" do

    it "returns empty array" do
      subject.delete(1)
      subject.find(:all).should == []
    end

  end # when try to delete a record with id = 1

  context "when try to create new record outside the range of partitions" do

    it "raises ActiveRecord::StatementInvalid" do
      lambda { subject.create_many([{ :created_at => DATE_NOW - 1.year, :company_id => 1 }])
      }.should raise_error(ActiveRecord::StatementInvalid)
    end

  end # when try to create new record outside the range of partitions

  context "when try to update a record outside the range of partitions" do

    it "raises ActiveRecord::StatementInvalid" do
      lambda { subject.update(1, :name => 'Kevin', :created_at => DATE_NOW - 1.year)
      }.should raise_error(ActiveRecord::StatementInvalid)
    end

  end # when try to update a record outside the range of partitions

  context "when try to find a record outside the range of partitions" do

    it "raises ActiveRecord::StatementInvalid" do
      lambda { subject.from_partition(DATE_NOW - 1.year).find(1)
      }.should raise_error(ActiveRecord::StatementInvalid)
    end

  end # when try to find a record outside the range of partitions
end # check that basic operations with postgres works correctly for time key
