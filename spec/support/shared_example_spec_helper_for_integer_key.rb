shared_examples_for "check that basic operations with postgres works correctly for integer key" do |class_name|

  let!(:subject) do
    class_name.reset_column_information
    class_name
  end

  context "when try to create one record" do

    it "record created" do
      expect { subject.create(:name => 'Phil', :company_id => 3, :integer_field => 2, :tags => ['IT', 'Finance'])
      }.not_to raise_error
    end

  end # when try to create one record

  context "when try to create one record using new/save" do

    it "record created" do
      expect {
        instance = subject.new(:name => 'Mike', :company_id => 1, :integer_field => 1, :tags => ['IT', 'Finance'])
        instance.save!
      }.not_to raise_error
    end

  end # when try to create one record using new/save

  context "when try to create many records" do

    it "records created" do
      expect { subject.create_many([
                                     { :name => 'Alex', :company_id => 2, :integer_field => 4 },
                                     { :name => 'Aaron', :company_id => 3, :integer_field => 2 }])
      }.not_to raise_error
    end

  end # when try to create many records

  context "when try to find a record with the search term is id" do

    it "returns employee name" do
      expect(subject.find(1).name).to eq("Keith")
    end

  end # when try to find a record with the search term is id

  context "when try to find a record with the search term is name" do

    it "returns employee name" do
      expect(subject.where(:name => 'Keith').first.name).to eq("Keith")
    end

  end # when try to find a record with the search term is name

  context "when try to find a record which is showing partition table" do

    it "returns employee name" do
      expect(subject.from_partition(1).find(1).name).to eq("Keith")
    end

  end # when try to find a record which is showing partition table

  context "when try to update a record with id = 1" do

    it "returns updated employee name" do
      subject.update(1, :name => 'Kevin')
      expect(subject.find(1).name).to eq("Kevin")
    end

    it "updates array attributes" do
      subject.update(1, :tags => ['Sales', 'Marketing'])
      result = subject.find(1)
      expect(result.tags).to eq ['Sales', 'Marketing']
    end

  end # when try to update a record with id = 1

  context "when try to update a record with update_many functions" do

    it "returns updated employee name" do
      subject.update_many( {
        { :id => 1, :integer_field => 1, :company_id => 1 } => {
            :name => 'Alex'
          }
      } )
      expect(subject.find(1).name).to eq("Alex")
    end

    it "returns updated employee name" do
      rows = [{
         :id => 1,
         :integer_field => 1,
         :company_id => 1,
         :name => 'Pit',
      }]

      options = {
        :set_array => '"name = datatable.name"',
        :where => '"#{table_name}.id = datatable.id"'
      }
      subject.update_many(rows, options)
      expect(subject.find(1).name).to eq("Pit")
    end

  end # when try to update a record with update_many functions

  context "when try to delete a record with id = 1" do

    it "returns empty array" do
      subject.delete(1)
      expect(subject.all).to eq([])
    end

  end # when try to delete a record with id = 1

  context "when try to create new record outside the range of partitions" do

    it "raises ActiveRecord::StatementInvalid" do
      expect { subject.create_many([{ :name => 'Mark', :company_id => 13, :integer_field => 5 } ])
      }.to raise_error(ActiveRecord::StatementInvalid)
    end

  end # when try to create new record outside the range of partitions

  context "when try to update a record outside the range of partitions" do

    it "raises ActiveRecord::RecordNotFound" do
      expect { subject.update(100500, :name => 'Kevin')
      }.to raise_error(ActiveRecord::RecordNotFound)
    end

  end # when try to update a record outside the range of partitions

  context "when try to find a record outside the range of partitions" do

    it "raises ActiveRecord::StatementInvalid" do
      expect { subject.from_partition(13).find(1)
      }.to raise_error(ActiveRecord::StatementInvalid)
    end

  end # when try to find a record outside the range of partitions

end # check that basic operations with postgres works correctly for integer key
