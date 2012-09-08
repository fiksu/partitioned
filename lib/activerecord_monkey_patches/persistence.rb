require 'active_record/persistence'

module ActiveRecord
  module Persistence
    def delete
      if persisted?
        self.class.from_partition(*self.class.table_partition_key_values(attributes)).delete(id)
        IdentityMap.remove(self) if IdentityMap.enabled?
      end
      @destroyed = true
      freeze
    end

    def destroy
      destroy_associations

      if persisted?
        IdentityMap.remove(self) if IdentityMap.enabled?
        pk         = self.class.primary_key
        column     = self.class.columns_hash[pk]
        substitute = connection.substitute_at(column, 0)

        relation = self.class.unscoped.where(
          arel_table[pk].eq(substitute))

        relation.bind_values = [[column, id]]
        relation.delete_all
      end

      @destroyed = true
      freeze
    end

    # Updates the associated record with values matching those of the instance attributes.
    # Returns the number of affected rows.
    def update(attribute_names = @attributes.keys)
      attributes_with_values = arel_attributes_values(false, false, attribute_names)
      return 0 if attributes_with_values.empty?
      klass = self.class
      using_arel_table = arel_table
      # This is pretty hacky: we adjust the attributes so they are connected to the correct arel_table
      # we can do this in two places and I've chosen here (which seems less intrusive).
      # Alternatively we could hook into any attribute change (model.created_at = Time.now.utc) and
      # adjust all arel_tables in all attributes when any of this model's partition key values change.
      # That seems like a lot of work.
      attributes_with_values = Hash[*attributes_with_values.map{|k,v| [using_arel_table[k.name], v]}.flatten]
      stmt = klass.unscoped.where(using_arel_table[klass.primary_key].eq(id)).arel.compile_update(attributes_with_values)
      klass.connection.update stmt
    end
    
    # Creates a record with values matching those of the instance attributes
    # and returns its id.
    def create
      if self.id.nil? && self.class.prefetch_primary_key?
        self.id = connection.next_sequence_value(self.class.sequence_name)
      end

      attributes_values = arel_attributes_values(!id.nil?)

      new_id = self.class.unscoped.insert attributes_values

      self.id ||= new_id if self.class.primary_key

      IdentityMap.add(self) if IdentityMap.enabled?
      @new_record = false
      id
    end
  end
end
