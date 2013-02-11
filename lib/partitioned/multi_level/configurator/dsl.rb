module Partitioned
  class MultiLevel
    #
    # Configuration manager for multi-level partitioning
    # it supports, the front-end UI (a Domain Specific Language) using {Dsl}
    # state using {Data}
    # and a parser using {Reader}
    #
    module Configurator
      # The Domain Specific Language UI manager for multi level partitioning classes 
      class Dsl < Partitioned::PartitionedBase::Configurator::Dsl
        # used to prevent use of invalid directives
        class InvalidForMultiLevelPartitioning < StandardError
          def initialize(model, dsl_key, remedy)
            super("#{model.name}: '#{dsl_key}' is not valid for multi-level partitioning.  #{remedy}")
          end
        end

        attr_reader :data, :model

        def initialize(most_derived_activerecord_class)
          super(most_derived_activerecord_class, Partitioned::MultiLevel::Configurator::Data)
          @using_classes = []
        end

        #
        # Definition of classes which will be used at multi level partitioning.
        #
        # @param [*Array<Class>] classes the classes, in order, used to partition this model
        # @return [optional]
        def using_classes(*classes)
          data.using_classes += [*classes]
        end

        # @raise [InvalidForMultiLevelPartitioning] always raised. `on` is not a valid DSL directive for multi-level partitioning
        def on(*ignored)
          raise InvalidForMultiLevelPartitioning.new(model, :on, "the partitioned keyword 'using' is used to define multi-level partitioned tables.")
        end
      end
    end
  end
end
