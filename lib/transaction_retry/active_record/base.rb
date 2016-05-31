require 'active_record/base'

module TransactionRetry
  module ActiveRecord
    module Base

      def self.included( base )
        base.extend( ClassMethods )
      end
      
      module ClassMethods
        
        def transaction_with_retry(*objects, &block)
          retry_count = 0

          begin
            transaction(*objects, &block)
          rescue ::ActiveRecord::TransactionIsolationConflict
            raise if retry_count >= TransactionRetry.max_retries
            raise if connection.open_transactions != 0
            
            retry_count += 1
            postfix = { 1 => 'st', 2 => 'nd', 3 => 'rd' }[retry_count] || 'th'
            logger.warn "Transaction isolation conflict detected. Retrying for the #{retry_count}-#{postfix} time..." if logger

            # Sleep 0, 1, 2, 4, ... seconds up to the TransactionRetry.max_retries.
            # Cap the sleep time at 32 seconds.
            seconds = TransactionRetry.wait_times[count-1] || 32
            sleep( seconds ) if seconds > 0
            retry
          end
        end
      end
    end
  end
end

ActiveRecord::Base.send( :include, TransactionRetry::ActiveRecord::Base )
