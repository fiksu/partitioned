module Partitioned
  # Partitioned abstract class for all partitioned models based as a single integer field value that is used as a foreign key
  class ByForeignKey < ByIntegerField
    self.abstract_class = true

    # the field to partition on
    # @return [Integer] re-routed to {#self.partition_foreign_key}
    def self.partition_integer_field
      return partition_foreign_key
    end

    # the field to partition on
    # @return [String] the name of the foreign key field
    def self.partition_foreign_key
      raise MethodNotImplemented.new(self, :partition_foreign_key)
    end

    partitioned do |partition|
      partition.foreign_key lambda {|model, *partition_key_values|
        return Configurator::Data::ForeignKey.new(model.partition_foreign_key,
                                                  ActiveSupport::Inflector::pluralize(model.partition_foreign_key.to_s.sub(/_id$/,'')),
                                                  :id)
      }
    end
  end
end
