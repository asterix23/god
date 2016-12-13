# Send an event to rabbit.
#
# msg    - The rabbit message

require('bunny')

CONTACT_DEPS[:rabbit] = ['json']
CONTACT_DEPS[:rabbit].each do |d|
  require d
end


module God
  module Contacts

    class Rabbit < Contact
      class << self
        attr_accessor :msg_host, :msg_port, :queue_name
      end

      @msgQueue = nil

      attr_accessor :msg_host, :msg_port, :queue_name

      def initialize()

        if(@msgQueue == nil)
          conn = Bunny.new({:host => arg(:msg_host), :port => arg(:msg_port)})
          conn.start
          ch = conn.create_channel

          @msgQueue = ch.direct(arg(:queue_name))
          #puts("rabbit initialize host = #{arg(:msg_host)}  port = #{arg(:msg_port)} queue = #{arg(:queue_name)}  @msgQueue = #{@msgQueue}")
        end
      rescue => e
        applog(nil, :info, "failed to send create Rabbit Queue  #{e.message}")
        applog(nil, :debug, e.backtrace.join("\n"))

        #puts("!!error in creating message queue: #{e.message}")
        #puts("backtrace: \n #{e.backtrace.join} \n")
      end


      def valid?
        valid = true
        valid &= complain("Attribute 'msg_host' must be specified", self) unless arg(:msg_host)
        valid &= complain("Attribute 'msg_port' must be specified", self) unless arg(:msg_port)
        valid &= complain("Attribute 'queue_name' must be specified", self) unless arg(:queue_name)

        #puts ("rabbit valid = #{valid}")
        valid
      end

      attr_accessor :msg

      def notify(message, time, priority, category, host)
        #puts("in notify - message = #{arg(:msg)}")

        #puts("notify: msgQueue = #{@msgQueue}")
        @msgQueue.publish(arg(:msg).to_json)

        self.info = "sent message: #{arg(:msg).to_json} to queue: #{@msgQueue}"
      rescue => e
        applog(nil, :info, "failed to send RabbitMQ message to queue  #{e.message}")
        applog(nil, :debug, e.backtrace.join("\n"))
      end
    end
  end
end
