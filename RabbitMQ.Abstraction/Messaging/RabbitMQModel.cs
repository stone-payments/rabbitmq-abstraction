using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;

namespace RabbitMQ.Abstraction.Messaging
{
    public class RabbitMQModel
    {
        private readonly Action<RabbitMQModel> _requeueModelAction;
        private readonly Action _discardModelAction;
        private readonly ILogger _logger;

        public IModel Model { get; set; }

        public RabbitMQModel(ILogger logger, IModel model, Action<RabbitMQModel> requeueModelAction, Action discardModelAction, bool subscribeEvents)
        {
            _logger = logger;
            Model = model;
            _requeueModelAction = requeueModelAction;
            _discardModelAction = discardModelAction;

            if (subscribeEvents)
                SubscribeModelEvents();
        }

        private void SubscribeModelEvents()
        {
            Model.CallbackException += Model_CallbackException;
            Model.BasicRecoverOk += Model_BasicRecoverOk;
            Model.ModelShutdown += ModelModelShutdown;
        }

        private void UnsubscribeModelEvents()
        {
            Model.CallbackException -= Model_CallbackException;
            Model.BasicRecoverOk -= Model_BasicRecoverOk;
            Model.ModelShutdown -= ModelModelShutdown;
        }

        private void ModelModelShutdown(object sender, ShutdownEventArgs e)
        {
            _logger?.LogInformation($"RabbitMQModel Shutdown. Cause: {e.Cause} ReplyText: {e.ReplyText} ");
        }

        private void Model_BasicRecoverOk(object sender, EventArgs e)
        {
            _logger?.LogInformation("RabbitMQModel Basic Recover Ok.");
        }

        private void Model_CallbackException(object sender, CallbackExceptionEventArgs e)
        {
            _logger?.LogError(e.Exception, $"RabbitMQModel Shutdown. Message: {e.Exception.Message}{Environment.NewLine}StackTrace: {e.Exception.StackTrace} ");
        }

        public void End()
        {
            Model.Dispose();
        }

        public void Dispose()
        {
            if (Model.IsOpen)
            {
                _requeueModelAction(this);
            }
            else
            {
                _discardModelAction();
                Model.Dispose();
            }
            UnsubscribeModelEvents();
        }

        public void Abort()
        {
            Model.Abort();
        }

        public void Abort(ushort replyCode, string replyText)
        {
            Model.Abort(replyCode, replyText);
        }

        public void BasicAck(ulong deliveryTag, bool multiple)
        {
            Model.BasicAck(deliveryTag, multiple);
        }

        public void BasicCancel(string consumerTag)
        {
            Model.BasicCancel(consumerTag);
        }

        public string BasicConsume(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object> arguments,
            IBasicConsumer consumer)
        {
            return Model.BasicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, consumer);
        }

        public BasicGetResult BasicGet(string queue, bool autoAck)
        {
            return Model.BasicGet(queue, autoAck);
        }

        public void BasicNack(ulong deliveryTag, bool multiple, bool requeue)
        {
            Model.BasicNack(deliveryTag, multiple, requeue);
        }

        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties, byte[] body)
        {
            Model.BasicPublish(exchange, routingKey, mandatory, basicProperties, body);
        }

        public void BasicPublish(string exchange, string routingKey, IBasicProperties basicProperties, byte[] body)
        {
            Model.BasicPublish(exchange, routingKey, basicProperties, body);
        }

        public void BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            Model.BasicQos(prefetchSize, prefetchCount, global);
        }

        public void BasicRecover(bool requeue)
        {
            Model.BasicRecover(requeue);
        }

        public void BasicRecoverAsync(bool requeue)
        {
            Model.BasicRecoverAsync(requeue);
        }

        public void BasicReject(ulong deliveryTag, bool requeue)
        {
            Model.BasicReject(deliveryTag, requeue);
        }

        public void Close()
        {
            Model.Close();
        }

        public void Close(ushort replyCode, string replyText)
        {
            Model.Close(replyCode, replyText);
        }

        public void ConfirmSelect()
        {
            Model.ConfirmSelect();
        }

        public IBasicProperties CreateBasicProperties()
        {
            return Model.CreateBasicProperties();
        }

        public void ExchangeBind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            Model.ExchangeBind(destination, source, routingKey, arguments);
        }

        public void ExchangeBindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            Model.ExchangeBindNoWait(destination, source, routingKey, arguments);
        }

        public void ExchangeDeclare(string exchange, string type, bool durable = false, bool autoDelete = false, IDictionary<string, object> arguments = null)
        {
            Model.ExchangeDeclare(exchange, type, durable, autoDelete, arguments);
        }

        public void ExchangeDeclareNoWait(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments)
        {
            Model.ExchangeDeclareNoWait(exchange, type, durable, autoDelete, arguments);
        }

        public void ExchangeDeclarePassive(string exchange)
        {
            Model.ExchangeDeclarePassive(exchange);
        }

        public void ExchangeDelete(string exchange, bool ifUnused)
        {
            Model.ExchangeDelete(exchange, ifUnused);
        }

        public void ExchangeDeleteNoWait(string exchange, bool ifUnused)
        {
            Model.ExchangeDeleteNoWait(exchange, ifUnused);
        }

        public void ExchangeUnbind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            Model.ExchangeUnbind(destination, source, routingKey, arguments);
        }

        public void ExchangeUnbindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            Model.ExchangeUnbindNoWait(destination, source, routingKey, arguments);
        }

        public void QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments = null)
        {
            Model.QueueBind(queue, exchange, routingKey, arguments);
        }

        public void QueueBindNoWait(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            Model.QueueBindNoWait(queue, exchange, routingKey, arguments);
        }

        public QueueDeclareOk QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments)
        {
            return Model.QueueDeclare(queue, durable, exclusive, autoDelete, arguments);
        }

        public void QueueDeclareNoWait(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments)
        {
            Model.QueueDeclareNoWait(queue, durable, exclusive, autoDelete, arguments);
        }

        public QueueDeclareOk QueueDeclarePassive(string queue)
        {
            return Model.QueueDeclarePassive(queue);
        }

        public uint MessageCount(string queue)
        {
            return Model.MessageCount(queue);
        }

        public uint ConsumerCount(string queue)
        {
            return Model.ConsumerCount(queue);
        }

        public uint QueueDelete(string queue, bool ifUnused = false, bool ifEmpty = false)
        {
            return Model.QueueDelete(queue, ifUnused, ifEmpty);
        }

        public void QueueDeleteNoWait(string queue, bool ifUnused, bool ifEmpty)
        {
            Model.QueueDeleteNoWait(queue, ifUnused, ifEmpty);
        }

        public uint QueuePurge(string queue)
        {
            return Model.QueuePurge(queue);
        }

        public void QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments)
        {
            Model.QueueUnbind(queue, exchange, routingKey, arguments);
        }

        public void TxCommit()
        {
            Model.TxCommit();
        }

        public void TxRollback()
        {
            Model.TxRollback();
        }

        public void TxSelect()
        {
            Model.TxSelect();
        }

        public bool WaitForConfirms()
        {
            return Model.WaitForConfirms();
        }

        public bool WaitForConfirms(TimeSpan timeout)
        {
            return Model.WaitForConfirms(timeout);
        }

        public bool WaitForConfirms(TimeSpan timeout, out bool timedOut)
        {
            return Model.WaitForConfirms(timeout, out timedOut);
        }

        public void WaitForConfirmsOrDie()
        {
            Model.WaitForConfirmsOrDie();
        }

        public void WaitForConfirmsOrDie(TimeSpan timeout)
        {
            Model.WaitForConfirmsOrDie(timeout);
        }

        public int ChannelNumber => Model.ChannelNumber;

        public ShutdownEventArgs CloseReason => Model.CloseReason;

        public IBasicConsumer DefaultConsumer
        {
            get => Model.DefaultConsumer;
            set => Model.DefaultConsumer = value;
        }

        public bool IsClosed => Model.IsClosed;

        public bool IsOpen => Model.IsOpen;

        public ulong NextPublishSeqNo => Model.NextPublishSeqNo;

        public TimeSpan ContinuationTimeout
        {
            get => Model.ContinuationTimeout;
            set => Model.ContinuationTimeout = value;
        }

        public event EventHandler<BasicAckEventArgs> BasicAcks
        {
            add => Model.BasicAcks += value;
            remove => Model.BasicAcks -= value;
        }

        public event EventHandler<BasicNackEventArgs> BasicNacks
        {
            add => Model.BasicNacks += value;
            remove => Model.BasicNacks -= value;
        }

        public event EventHandler<EventArgs> BasicRecoverOk
        {
            add => Model.BasicRecoverOk += value;
            remove => Model.BasicRecoverOk -= value;
        }

        public event EventHandler<BasicReturnEventArgs> BasicReturn
        {
            add => Model.BasicReturn += value;
            remove => Model.BasicReturn -= value;
        }

        public event EventHandler<CallbackExceptionEventArgs> CallbackException
        {
            add => Model.CallbackException += value;
            remove => Model.CallbackException -= value;
        }

        public event EventHandler<FlowControlEventArgs> FlowControl
        {
            add => Model.FlowControl += value;
            remove => Model.FlowControl -= value;
        }

        public event EventHandler<ShutdownEventArgs> ModelShutdown
        {
            add => Model.ModelShutdown += value;
            remove => Model.ModelShutdown -= value;
        }
    }
}