// vim: ts=2 sw=2 expandtab
#include "TinyMqtt.h"
#include <sstream>

#if TINY_MQTT_DEBUG
static auto cyan = TinyConsole::cyan;
static auto white = TinyConsole::white;
static auto red = TinyConsole::red;
static auto yellow = TinyConsole::yellow;

int TinyMqtt::debug=2;

#endif

#ifdef EPOXY_DUINO
  std::map<MqttMessage::Type, int> MqttClient::counters;
  int MqttBroker::instances = 0;
  int MqttClient::instances = 0;

#endif

MqttBroker::MqttBroker(uint16_t port, uint8_t max_retain_size)
{
  debug("New broker" << port);
  retain_size = max_retain_size;
  server = new TcpServer(port);
#ifdef TINY_MQTT_ASYNC
  server->onClient(onClient, this);
#endif
#ifdef EPOXY_DUINO
  instances++;
#endif
}

MqttBroker::~MqttBroker()
{
#ifdef EPOXY_DUINO
  instances--;
#endif
  closeRemoteBroker();
  while(clients.size())
  {
    auto client = clients[0];
    client->local_broker = nullptr;
    if (client->cltFlags & MqttClient::CltFlags::CltFlagToDelete)
    {
      // std::cout << "Deleting client" << std::endl;
      delete client;
    }
    clients.erase(clients.begin());
  }
  delete server;
}

// private constructor used by broker only
MqttClient::MqttClient(MqttBroker* local_broker, TcpClient* new_client)
  : local_broker(local_broker)
{
  debug("MqttClient private with broker");
#ifdef TINY_MQTT_ASYNC
  tcp_client = new_client;
  tcp_client->onData(onData, this);
  // client->onConnect() TODO
  // client->onDisconnect() TODO
#else
  tcp_client = new TcpClient(*new_client);
#endif
#ifdef EPOXY_DUINO
  alive = millis()+500000;
  instances++;
#else
  alive = millis()+5000;  // TODO MAGIC client expires after 5s if no CONNECT msg
#endif
}

MqttClient::MqttClient(MqttBroker* local_broker, const string& id)
  : local_broker(local_broker), clientId(id)
{
  alive = 0;
  keep_alive = 0;

  if (local_broker) local_broker->addClient(this);
#ifdef EPOXY_DUINO
  instances++;
#endif
}

MqttClient::~MqttClient()
{
#ifdef EPOXY_DUINO
  instances--;
#endif
  close();
  delete tcp_client;
  debug("*** MqttClient delete()");
}

void MqttClient::close(bool bSendDisconnect)
{
  debug("close " << id().c_str());
  resetFlag(CltFlagConnected);
  if (tcp_client)  // connected to a remote broker
  {
    if (bSendDisconnect and tcp_client->connected())
    {
      message.create(MqttMessage::Type::Disconnect);
      message.hexdump("close");
      message.sendTo(this);
    }
    tcp_client->stop();
    tcp_client = nullptr;        // <— important: isAlive() will now be false
  }

  if (local_broker)
  {
    local_broker->removeClient(this);
    local_broker = nullptr;
  }
}

void MqttClient::connect(MqttBroker* local)
{
  debug("MqttClient::connect_local");
  close();
  local_broker = local;
  local_broker->addClient(this);
}

void MqttClient::connect(string broker, uint16_t port, uint16_t ka)
{
  debug("MqttClient::connect_to_host " << broker << ':' << port);
  keep_alive = ka;
  close();
  if (tcp_client) delete tcp_client;
  tcp_client = new TcpClient;

#ifdef TINY_MQTT_ASYNC
  tcp_client->onData(onData, this);
  tcp_client->onConnect(onConnect, this);
  tcp_client->connect(broker.c_str(), port);
#else
  if (tcp_client->connect(broker.c_str(), port))
  {
    debug("link established");
    onConnect(this, tcp_client);
  }
  else
  {
    debug("unable to connect.");
  }
#endif
}

// --- retainDrop: no-op to satisfy linker (we don't use retained msgs) ---
void MqttBroker::retainDrop() {
  // Intentionally empty: retained messages disabled/unused in this build.
}

// TinyMqtt.cpp — add this definition once
// Signature must match TinyMqtt.h exactly.
bool MqttBroker::compareString(const char* a, const char* b, uint8_t h) const
{
  if (!a || !b) return false;

  if (h == 0) {
    // Fallback: both are expected to be NUL-terminated
    return strcmp(a, b) == 0;
  }

  // Compare exactly the first h bytes
  if (strncmp(a, b, h) != 0) return false;

  // Optional strictness: treat equality as “same h-length string”
  // i.e., both are either exactly h bytes long or the next byte is a terminator.
  const char ta = a[h];
  const char tb = b[h];
  const bool a_end = (ta == '\0' || ta == '\r' || ta == '\n');
  const bool b_end = (tb == '\0' || tb == '\r' || tb == '\n');

  return a_end && b_end;
}


void MqttBroker::addClient(MqttClient* client)
{
  debug("MqttBroker::addClient");
  clients.push_back(client);
}

void MqttBroker::closeRemoteBroker()
{
  if (remote_broker)
  {
    delete remote_broker;
    remote_broker = nullptr;
  }
}

void MqttBroker::connect(const string& host, uint16_t port)
{
  debug("MqttBroker::connect");
  closeRemoteBroker();
  if (remote_broker == nullptr) remote_broker = new MqttClient;
  remote_broker->connect(host, port);
  remote_broker->local_broker = this;  // Because connect removed the link
  // TODO shouldn't we resubscribe to all client subscriptions ?
}

void MqttBroker::removeClient(MqttClient* remove)
{
  debug("removeClient");
  for(auto it=clients.begin(); it!=clients.end(); it++)
  {
    auto client=*it;
    if (client==remove)
    {
      // TODO if this broker is connected to an external broker
      // we have to unsubscribe remove's topics.
      // (but doing this, check that other clients are not subscribed...)
      // Unless -> we could receive useless messages
      //        -> we are using (memory) one IndexedString plus its string for nothing.
      debug("Remove " << clients.size());
      clients.erase(it);
      debug("Client removed " << clients.size());
      return;
    }
  }
  debug(red << "Error cannot remove client");  // TODO should not occur
}

void MqttBroker::onClient(void* broker_ptr, TcpClient* client)
{
  debug("MqttBroker::onClient");
  MqttBroker* broker = static_cast<MqttBroker*>(broker_ptr);

  MqttClient* mqtt = new MqttClient(broker, client);
  mqtt->setFlag(MqttClient::CltFlags::CltFlagToDelete);
  broker->addClient(mqtt);
  debug("New client");
}

void MqttBroker::loop()
{
#ifndef TINY_MQTT_ASYNC
  TcpClient client = server->accept();

  if (client)
  {
    onClient(this, &client);
  }
#endif
  if (remote_broker)
  {
    // TODO should monitor broker's activity.
    // 1 When broker disconnect and reconnect we have to re-subscribe
    remote_broker->loop();
  }

  for(size_t i=0; i<clients.size(); i++)
  {
    MqttClient* client = clients[i];
    if (client->connected())
    {
      client->loop();
    }
    else
    {
      debug("Client " << client->id().c_str() << "  Disconnected, local_broker=" << (dbg_ptr)client->local_broker);
      // Note: deleting a client not added by the broker itself will probably crash later.
      delete client;
      break;
    }
  }
}

// Obvioulsy called when the broker is connected to another broker.
MqttError MqttBroker::subscribe(MqttClient* client, const Topic& topic, uint8_t qos)
{
  debug("MqttBroker::subscribe to " << topic.str() << ", retained=" << retained.size() );
  for(auto& retainItem: retained)
  {
    auto &retained_topic = retainItem.first;
    auto &retain = retainItem.second;
    debug("  retained: " << retained_topic.str());
    if (topic.matches(retained_topic))
    {
      debug("  -> sending");
      client->publishIfSubscribed(retained_topic, retain.msg);
    }
  }
  if (remote_broker && remote_broker->connected())
  {
    return remote_broker->subscribe(topic, qos);
  }
  return MqttNowhereToSend;
}

MqttError MqttBroker::publish(const MqttClient* source, const Topic& topic, MqttMessage& msg)
{
  MqttError retval = MqttOk;

  // Keep your retained store as-is (original message)
  retain(topic, msg);

  // ---- Parse incoming PUBLISH once -----------------------------------------
  const char* vhdr = msg.getVHeader();   // start of variable header (topic length)
  const char* p    = vhdr;
  uint16_t    tlen = 0;
  msg.getString(p, tlen);                // advances 'p' to the MQTT string payload
  const char* afterTopic = p + tlen;

  // Read QoS/retain from fixed flags
  uint8_t f   = msg.flags();
  uint8_t qos = (f >> 1) & 0x03;
  bool    hasPktId = (qos > 0);

  const char* payloadPtr = afterTopic + (hasPktId ? 2 : 0);
  size_t      payloadLen = (size_t)(msg.end() - payloadPtr);
  // --------------------------------------------------------------------------

  debug("MqttBroker::publish");

  int i = 0;
  for (auto client : clients)
  {
    i++;

#if TINY_MQTT_DEBUG
    Console << __LINE__ << " broker:" << (remote_broker && remote_broker->connected() ? "linked" : "alone")
            << "  srce=" << (source->isLocal() ? "loc" : "rem")
            << " clt#" << i << ", local=" << client->isLocal()
            << ", con=" << client->connected() << endl;
#endif

    bool fanout_to_local_clients = false;

    if (remote_broker && remote_broker->connected())
    {
      // If this broker is linked upstream:
      if (source == remote_broker) {
        // From upstream → deliver to local clients too
        fanout_to_local_clients = true;
      } else {
        // From local → forward upstream as-is (don’t mutate)
        if (remote_broker && remote_broker->isAlive()) {
          MqttError ret = remote_broker->publishIfSubscribed(topic, msg);
          if (ret != MqttOk) retval = ret;
        } else {
            debug("skip dead remote_broker");
        }
      }
    }
    else
    {
      // Not linked upstream → deliver locally
      fanout_to_local_clients = true;
    }

#if TINY_MQTT_DEBUG
    Console << ", doit=" << fanout_to_local_clients << ' ';
#endif

    if (!fanout_to_local_clients) {
      debug("");
      continue;
    }

    // ------- Build a QoS0 copy for local subscribers -----------------------
    // TinyMQTT behaves like a QoS0 broker for local fan-out.
    // We ALWAYS send QoS0 to subscribers (no Packet Identifier, DUP=0, RETAIN=0),
    // regardless of incoming QoS/retain, to avoid protocol errors with strict clients.
    MqttMessage out(MqttMessage::Type::Publish);

    // MQTT string for topic: 2-byte big-endian length + bytes
    const std::string& tstr = topic.str();
    uint16_t tlen_be = (uint16_t)tstr.size();
    out.add((uint8_t)((tlen_be >> 8) & 0xFF));
    out.add((uint8_t)(tlen_be & 0xFF));
    if (tlen_be) {
      out.add(tstr.data(), tstr.size(), false);  // don't copy internally if your API allows
    }

    // QoS0 publish has NO Packet Identifier

    // Append payload as-is
    if (payloadLen) {
      out.add(payloadPtr, payloadLen, false);
    }


    // Skip dead sockets (prevents tcp_output() crash paths)
    if (!client || !client->isAlive()) {
      debug("skip dead client");
      continue;
    }

    // Fan-out to this client if subscribed
    MqttError ret2 = client->publishIfSubscribed(topic, out);
    if (ret2 == MqttNowhereToSend) {
      // Socket likely died between the liveness check and write; skip further sends to it.
      debug("client became dead during send");
      continue;
    }
    if (ret2 != MqttOk) {
      retval = ret2;   // keep last non-OK
    }
    debug("");
  }

  return retval;
}


void MqttMessage::getString(const char* &buff, uint16_t& len)
{
  len = getSize(buff);
  buff+=2;
}

void MqttClient::clientAlive(uint32_t more_seconds)
{
  debug("MqttClient::clientAlive");
  if (keep_alive)
  {
#ifdef EPOXY_DUINO
    alive=millis()+500000+0*more_seconds;
#else
    alive=millis()+1000*(keep_alive+more_seconds);
#endif
  }
  else
    alive=0;
}

void MqttClient::loop()
{
  if (keep_alive && (millis() >= alive))
  {
    if (tcp_client && tcp_client->connected())
    {
      debug("pingreq");
      static MqttMessage pingreq(MqttMessage::Type::PingReq);
      pingreq.sendTo(this);
      clientAlive(0);

      // TODO when many MqttClient passes through a local broker
      // there is no need to send one PingReq per instance.
    }
    else if (local_broker)
    {
      debug(red << "timeout client");
      close();
      debug(red << "closed");
    }
  }

#ifndef TINY_MQTT_ASYNC
  while(tcp_client && tcp_client->available()>0)
  {
    message.incoming(tcp_client->read());
    if (message.type())
    {
      processMessage(&message);
      message.reset();
    }
  }
#endif
}

void MqttClient::onConnect(void *mqttclient_ptr, TcpClient*)
{
  MqttClient* mqtt = static_cast<MqttClient*>(mqttclient_ptr);
  debug("MqttClient::onConnect");
  MqttMessage msg(MqttMessage::Type::Connect);
  msg.add("MQTT",4);
  msg.add(0x4);  // Mqtt protocol version 3.1.1
  msg.add(0x0);  // Connect flags         TODO user / name

  msg.add((char)(mqtt->keep_alive >> 8));   // keep_alive
  msg.add((char)(mqtt->keep_alive & 0xFF));
  msg.add(mqtt->clientId);
  debug("cnx: mqtt connecting");
  msg.sendTo(mqtt);
  msg.reset();
  debug("cnx: mqtt sent " << (dbg_ptr)mqtt->local_broker);

  mqtt->clientAlive(0);
}

#ifdef TINY_MQTT_ASYNC
void MqttClient::onData(void* client_ptr, TcpClient*, void* data, size_t len)
{
  char* char_ptr = static_cast<char*>(data);
  MqttClient* client=static_cast<MqttClient*>(client_ptr);
  while(len>0)
  {
    client->message.incoming(*char_ptr++);
    if (client->message.type())
    {
      client->processMessage(&client->message);
      client->message.reset();
    }
    len--;
  }
}
#endif

void MqttClient::resubscribe()
{
  // TODO resubscription limited to 256 bytes
  if (subscriptions.size())
  {
    MqttMessage msg(MqttMessage::Type::Subscribe, 2);

    // TODO manage packet identifier
    msg.add(0);
    msg.add(0);

    for(auto topic: subscriptions)
    {
      msg.add(topic);
      msg.add(0);    // TODO qos
    }
    if (!isAlive()) { return; }
    msg.sendTo(this);  // TODO return value
  }
}

MqttError MqttClient::subscribe(Topic topic, uint8_t qos)
{
  debug("MqttClient::subsribe(" << topic.c_str() << ")");
  MqttError ret = MqttOk;

  subscriptions.insert(topic);

  if (local_broker==nullptr) // connected to a remote broker
  {
    return sendTopic(topic, MqttMessage::Type::Subscribe, qos);
  }
  else
  {
    return local_broker->subscribe(this, topic, qos);
  }
  return ret;
}

MqttError MqttClient::unsubscribe(Topic topic)
{
  debug("MqttClient::unsubscribe");
  auto it=subscriptions.find(topic);
  if (it != subscriptions.end())
  {
    subscriptions.erase(it);
    if (local_broker==nullptr) // remote broker
    {
      return sendTopic(topic, MqttMessage::Type::UnSubscribe, 0);
    }
  }
  return MqttOk;
}

MqttError MqttClient::sendTopic(const Topic& topic, MqttMessage::Type type, uint8_t qos)
{
  debug("MqttClient::sendTopic");
  MqttMessage msg(type, 2);

  // TODO manage packet identifier
  msg.add(0);
  msg.add(0);

  msg.add(topic);
  msg.add(qos);

  // TODO instead we should wait (state machine) for SUBACK / UNSUBACK ?
  return msg.sendTo(this);
}

void MqttClient::processMessage(MqttMessage* mesg)
{
#if TINY_MQTT_DEBUG
  mesg->hexdump("Incoming");
#endif
  auto header = mesg->getVHeader();
  const char* payload;
  uint16_t len;
  bool bclose = true;

#ifdef EPOXY_DUINO
  counters[mesg->type()]++;
#endif

  switch (mesg->type())
  {
    case MqttMessage::Type::Connect:
      if (mqtt_connected()) { debug("already connected"); break; }
      payload    = header + 10;
      mqtt_flags = header[7];
      keep_alive = MqttMessage::getSize(header + 8);

      if (strncmp("MQTT", header + 2, 4)) { debug("bad mqtt header"); break; }
      if (header[6] != 0x04) { debug("Unsupported MQTT version (" << (int)header[6] << "), only version=4 supported" << endl); break; }

      // ClientId
      mesg->getString(payload, len);
      clientId = string(payload, len);
      payload += len;

      if (mqtt_flags & FlagWill) { mesg->getString(payload, len); payload += len; mesg->getString(payload, len); payload += len; }

      if (mqtt_flags & FlagUserName) { mesg->getString(payload, len); if (!local_broker->checkUser(payload, len)) break; payload += len; }
      if (mqtt_flags & FlagPassword) { mesg->getString(payload, len); if (!local_broker->checkPassword(payload, len)) break; payload += len; }

#if TINY_MQTT_DEBUG
      Console << yellow << "Client " << clientId << " connected : keep alive=" << keep_alive << '.' << white << endl;
#endif
      bclose = false;
      setFlag(CltFlagConnected);

      // Send CONNACK via guarded sendTo
            {
        MqttMessage msg(MqttMessage::Type::ConnAck);
        msg.add(0);  // Session present
        msg.add(0);  // Accepted
        if (!isAlive()) { bclose = true; break; }
        if (msg.sendTo(this) != MqttOk) { bclose = true; }
      }
      break;

    case MqttMessage::Type::ConnAck:
      setFlag(CltFlagConnected);
      bclose = false;
      resubscribe();
      break;

    case MqttMessage::Type::SubAck:
    case MqttMessage::Type::PubAck:
      if (!mqtt_connected()) break;
      bclose = false;
      break;

    case MqttMessage::Type::PingResp:
      bclose = false;
      break;

    case MqttMessage::Type::PingReq:
      if (!mqtt_connected()) break;
      {
          MqttMessage resp(MqttMessage::Type::PingResp);
          if (!(tcp_client && tcp_client->connected())) { bclose = true; break; }
          resp.sendTo(this);
          bclose = false;
      }
      break;

    case MqttMessage::Type::Subscribe:
    case MqttMessage::Type::UnSubscribe:
    {
      if (!mqtt_connected()) break;
      payload = header + 2;

      debug("un/subscribe loop");
      string qoss;
      while (payload < mesg->end())
      {
        mesg->getString(payload, len);  // Topic
        debug("  topic (" << string(payload, len) << ')');
        Topic topic(payload, len);
        payload += len;

        if (mesg->type() == MqttMessage::Type::Subscribe) {
          uint8_t qos = *payload++;
          if (qos != 0) { debug("Unsupported QOS" << qos << endl); qoss.push_back(0x80); }
          else           { qoss.push_back(qos); }
          subscribe(topic);
        } else {
          auto it = subscriptions.find(topic);
          if (it != subscriptions.end()) subscriptions.erase(it);
        }
      }
      debug("end loop");
      bclose = false;

      // Packet Identifier is the first two bytes of the SUBSCRIBE/UNSUBSCRIBE variable header
      const uint8_t pid_msb = static_cast<uint8_t>(header[0]);
      const uint8_t pid_lsb = static_cast<uint8_t>(header[1]);

      if (mesg->type() == MqttMessage::Type::Subscribe) {
        // ----- SUBACK (variable header: Packet Id; payload: N return codes) -----
        MqttMessage ack(MqttMessage::Type::SubAck);
        ack.add(pid_msb);
        ack.add(pid_lsb);
        // Ensure payload length == number of topics parsed
        // qoss contains 0x00 for QoS0 or 0x80 for failure
        if (!qoss.empty()) {
          ack.add(qoss.c_str(), qoss.size(), false);
        }
        // Send via hardened path
        if (!(tcp_client && tcp_client->connected())) { bclose = true; break; }
        if (ack.sendTo(this) != MqttOk)                { bclose = true; break; }

      } else {
        // ----- UNSUBACK (variable header: Packet Id; payload: EMPTY) -----
        MqttMessage ack(MqttMessage::Type::UnSuback);
        ack.add(pid_msb);
        ack.add(pid_lsb);
        if (!(tcp_client && tcp_client->connected())) { bclose = true; break; }
        if (ack.sendTo(this) != MqttOk)               { bclose = true; break; }
      }

      break;
    }

    case MqttMessage::Type::UnSuback:
      if (!mqtt_connected()) break;
      bclose = false;
      break;

    case MqttMessage::Type::Publish:
    {
#if TINY_MQTT_DEBUG
      Console << "publish " << mqtt_connected() << '/' << (long)tcp_client << endl;
#endif
      if (mqtt_connected() || tcp_client == nullptr)
      {
        uint8_t qos = mesg->flags(); qos = (qos / 2) & 3;

        payload = header;
        mesg->getString(payload, len);
        Topic published(payload, len);
        payload += len;

#if TINY_MQTT_DEBUG
        Console << "Received Publish (" << published.str().c_str() << ") size=" << (int)len << endl;
#endif

        const char* ID = nullptr;     // Packet Identifier (QoS1/2)
        if (qos) { ID = payload; payload += 2; }
        len = mesg->end() - payload;

       if (qos == 1)
        {
          MqttMessage msg(MqttMessage::Type::PubAck);
          uint8_t id_hi = 0, id_lo = 0;
          if (ID) { id_hi = static_cast<uint8_t>(ID[0]); id_lo = static_cast<uint8_t>(ID[1]); }
          msg.add(id_hi);
          msg.add(id_lo);
          if (!isAlive()) { bclose = true; break; }
          if (msg.sendTo(this) != MqttOk) { bclose = true; break; }
        }


        if (local_broker == nullptr || tcp_client == nullptr) {
          if (callback && isSubscribedTo(published)) {
            callback(this, published, payload, len);
          }
        } else {
          if (!isAlive()) { bclose = true; break; }
          debug("publishing to local_broker");
          local_broker->publish(this, published, *mesg);
        }

        bclose = false;
      }
      break;
    }

    case MqttMessage::Type::Disconnect:
      if (!mqtt_connected()) break;
      resetFlag(CltFlagConnected);
      close(false);
      bclose = false;
      break;

    default:
      bclose = true;
      break;
  };

  if (bclose) {
#if TINY_MQTT_DEBUG
    Console << red << "*************** Error msg 0x" << _HEX(mesg->type());
    mesg->hexdump("-------ERROR ------");
    dump();
    Console << white << endl;
#endif
    close();
  } else {
    clientAlive(local_broker ? 5 : 0);
  }
}


bool Topic::matches(const Topic& topic) const
{
  if (getIndex() == topic.getIndex()) return true;
  const char* p1 = c_str();
  const char* p2 = topic.c_str();

  if (p1 == p2) return true;
  if (*p2 == '$' and *p1 != '$') return false;

  while(*p1 and *p2)
  {
    if (*p1 == '+')
    {
      ++p1;
      if (*p1 and *p1!='/') return false;
      if (*p1) ++p1;
      while(*p2 and *p2++!='/');
    }
    else if (*p1 == '#')
    {
      if (*++p1==0) return true;
      return false;
    }
    else if (*p1 == '*')
    {
      const char c=*(p1+1);
      if (c==0) return true;
      if (c!='/') return false;
      const char*p = p1+2;
      while(*p and *p2)
      {
        if (*p == *p2)
        {
          if (*p==0) return true;
          if (*p=='/')
          {
            p1=p;
            break;
          }
        }
        else
        {
          while(*p2 and *p2++!='/');
          break;
        }
        ++p;
        ++p2;
      }
      if (*p==0) return true;
    }
    else if (*p1 == *p2)
    {
      ++p1;
      ++p2;
    }
    else
      return false;
  }
  if (*p1=='/' and p1[1]=='#' and p1[2]==0) return true;
  return *p1==0 and *p2==0;
}


// publish from local client
MqttError MqttClient::publish(const Topic& topic, const char* payload, size_t pay_length, bool retain)
{
  MqttMessage msg(MqttMessage::Publish, retain ? 1 : 0);
  msg.add(topic);
  msg.add(payload, pay_length, false);
  msg.complete();

  if (local_broker)
  {
    if (!isAlive()) { return MqttNowhereToSend; }
    return local_broker->publish(this, topic, msg);
  }
  else if (tcp_client and connected())
    return msg.sendTo(this);
  else
    return MqttNowhereToSend;
}

// republish a received publish if it matches any in subscriptions

MqttError MqttClient::publishIfSubscribed(const Topic& topic, MqttMessage& msg)
{
  // Don’t even try if the client isn’t alive
  if (!(tcp_client && tcp_client->connected())) return MqttNowhereToSend;

  if (!isSubscribedTo(topic)) return MqttOk;

  // Send via the already-hardened sendTo()
  return msg.sendTo(this);
}

bool MqttClient::isSubscribedTo(const Topic& topic) const
{
  for(const auto& subscription: subscriptions)
    if (subscription.matches(topic))
      return true;

  return false;
}

void MqttMessage::reset()
{
  buffer.clear();
  state=FixedHeader;
  size=0;
}

void MqttMessage::incoming(char in_byte)
{
  buffer += in_byte;
  switch(state)
  {
    case FixedHeader:
      size=MaxBufferLength;
      state = Length;
      break;
    case Length:

      if (size==MaxBufferLength)
        size = in_byte & 0x7F;
      else
        size += static_cast<uint16_t>(in_byte & 0x7F)<<7;

      if (size > MaxBufferLength)
        state = Error;
      else if ((in_byte & 0x80) == 0)
      {
        vheader = buffer.length();
        if (size==0)
          state = Complete;
        else
        {
          buffer.reserve(size);
          state = VariableHeader;
        }
      }
      break;
    case VariableHeader:
    case PayLoad:
      --size;
      if (size==0)
      {
        state=Complete;
        // hexdump("rec");
      }
      break;
    case Create:
      size++;
      break;
    case Complete:
    default:
      #if TINY_MQTT_DEBUG
        Console << red << "Spurious " << _HEX(in_byte) << white << endl;
        hexdump("spurious");
      #endif
      reset();
      break;
  }
  if (buffer.length() > MaxBufferLength)
  {
    debug("Too long " << state);
    reset();
  }
}

void MqttMessage::add(const char* p, size_t len, bool addLength)
{
  if (addLength)
  {
    buffer.reserve(buffer.length()+2);
    incoming(len>>8);
    incoming(len & 0xFF);
  }
  while(len--) incoming(*p++);
}

void MqttMessage::encodeLength()
{
  debug("encodingLength");
  if (state != Complete)
  {
    int length = buffer.size()-3;  // 3 = 1 byte for header + 2 bytes for pre-reserved length field.
    if (length <= 0x7F)
    {
      buffer.erase(1,1);
      buffer[1] = length;
      vheader = 2;
    }
    else
    {
      buffer[1] = 0x80 | (length & 0x7F);
      buffer[2] = (length >> 7);
      vheader = 3;
    }

    // We could check that buffer[2] < 128 (end of length encoding)
    state = Complete;
  }
};

MqttError MqttMessage::sendTo(MqttClient* client)
{
  if (!client) return MqttNowhereToSend;
  if (buffer.empty()) return MqttInvalidMessage;
  if (!client->isAlive()) return MqttNowhereToSend;

  // Encode Remaining Length once
  encodeLength();

  // --- Parse fixed header (after encodeLength) -------------------------------
  // buffer layout now: [byte0=type|flags][RL varint (1..4 bytes)] [rest...]
  const uint8_t* raw   = reinterpret_cast<const uint8_t*>(buffer.data());
  const size_t   blen  = buffer.size();
  if (blen < 2) return MqttInvalidMessage;

  uint8_t h0 = raw[0];
  uint8_t mtype = (h0 >> 4) & 0x0F;
  uint8_t flags =  h0       & 0x0F;

  // Decode Remaining Length (varint)
  size_t rl = 0, mul = 1;
  size_t rl_bytes = 0;
  for (size_t i = 1; i < blen && i <= 4; ++i) {
    ++rl_bytes;
    rl += (raw[i] & 0x7F) * mul;
    if ((raw[i] & 0x80) == 0) break;
    mul *= 128;
  }
  size_t header_bytes = 1 + rl_bytes;
  if (header_bytes > blen) return MqttInvalidMessage;
  if (rl != (blen - header_bytes)) {
    // Remaining length must match payload size
    return MqttInvalidMessage;
  }

  // --- Per-type quick validation --------------------------------------------
  const uint8_t* body = raw + header_bytes;
  const size_t   body_len = rl;

  auto need_exact = [&](size_t n)->bool { return body_len == n; };
  auto need_min   = [&](size_t n)->bool { return body_len >= n; };

  switch (mtype) {
    case 2: { // CONNACK
      // Must be exactly 2 bytes: [session_present][return_code]
      if (!need_exact(2)) return MqttInvalidMessage;
      break;
    }
    case 3: { // PUBLISH
      uint8_t qos = (flags >> 1) & 0x03;
      if (qos != 0) return MqttInvalidMessage;

      if (!need_min(2)) return MqttInvalidMessage;
 
      uint16_t tlen16 = (static_cast<uint16_t>(body[0]) << 8) | body[1];
      size_t   tlen   = static_cast<size_t>(tlen16);
      if ((size_t)2 + tlen > body_len) return MqttInvalidMessage;

      // QoS0 must NOT include a Packet Identifier — our fan-out builds QoS0 already.
      break;
    }
    case 4: { // PUBACK
      // Must carry exactly a 2-byte Packet Identifier
      if (!need_exact(2)) return MqttInvalidMessage;
      break;
    }
    case 9: { // SUBACK
      // At least 2 bytes for Packet Identifier; payload: N return codes
      if (!need_min(2)) return MqttInvalidMessage;
      // No further strict check here (we can add one if you count topics earlier)
      break;
    }
    case 11: { // UNSUBACK
      // Exactly 2 bytes: Packet Identifier; NO payload
      if (!need_exact(2)) return MqttInvalidMessage;
      break;
    }
    case 13: { // PINGREQ — we never send this from broker
      return MqttInvalidMessage;
    }
    case 14: { // PINGRESP
      // No payload
      if (!need_exact(0)) return MqttInvalidMessage;
      break;
    }
    default:
      // Other types: accept as-is (or tighten later if needed)
      break;
  }

#if TINY_MQTT_DEBUG
  hexdump("Sending ");
#endif

  // Chunked, liveness-aware write (hardened client->write)
  const char* p = buffer.data();
  size_t n = buffer.size();
  const size_t CHUNK = 256;
  size_t off = 0;

  while (off < n) {
    if (!client->isAlive()) return MqttNowhereToSend;
    size_t take = (n - off < CHUNK) ? (n - off) : CHUNK;
    client->write(p + off, take);
    off += take;
    yield();
  }
  return MqttOk;
}


void MqttBroker::retain(const Topic& topic, const MqttMessage& msg)
{
  debug("MqttBroker::retain msg_type=" << _HEX(msg.type()) << ", retain_size=" << retain_size);
  if (retain_size==0 or msg.type() != MqttMessage::Publish) return;
  if (msg.flags() & 1)  // flag RETAIN
  {
    debug("  retaining " << topic.str());
    auto old = retained.find(topic);
    if (old == retained.end())
      retainDrop();
    else
      retained.erase(old);
    // FIXME if payload size == 0 remove message from retained
    Retain r(micros(), msg);
    r.msg.retained();
    retained.insert({ topic, std::move(r)});
  }
}

void MqttMessage::hexdump(const char* prefix) const
{
  (void)prefix;
#if TINY_MQTT_DEBUG
  if (TinyMqtt::debug<2) return;
  static std::map<Type, string> tts={
    { Connect, "Connect" },
    { ConnAck, "Connack" },
    { Publish, "Publish" },
    { PubAck,  "Puback" },
    { Subscribe, "Subscribe" },
    { SubAck,   "Suback" },
    { UnSubscribe, "Unsubscribe" },
    { UnSuback, "Unsuback" },
    { PingReq, "Pingreq" },
    { PingResp, "Pingresp" },
    { Disconnect, "Disconnect" }
  };
  string t("Unknown");
  Type typ=static_cast<Type>(buffer[0] & 0xF0);
  if (tts.find(typ) != tts.end())
    t=tts[typ];
  Console.fg(cyan);
#ifdef NOT_ESP_CORE
  Console << "---> MESSAGE " << t << ' ' << _HEX(typ) << ' ' << " mem=???" << endl;
#else
  Console << "---> MESSAGE " << t << ' ' << _HEX(typ) << ' ' << " mem=" << ESP.getFreeHeap() << endl;
#endif
  Console.fg(white);

  uint16_t addr=0;
  const int bytes_per_row = 8;
  const char* hex_to_str = " | ";
  const char* separator = hex_to_str;
  const char* half_sep = " - ";
  string ascii;

  Console << prefix << " size(" << buffer.size() << "), state=" << state << endl;

  for(const char chr: buffer)
  {
    if ((addr % bytes_per_row) == 0)
    {
      if (ascii.length()) Console << hex_to_str << ascii << separator << endl;
      if (prefix) Console << prefix << separator;
      ascii.clear();
    }
    addr++;
    if (chr<16) Console << '0';
    Console << _HEX(chr) << ' ';

    ascii += (chr<32 ? '.' : chr);
    if (ascii.length() == (bytes_per_row/2)) ascii += half_sep;
  }
  if (ascii.length())
  {
    while(ascii.length() < bytes_per_row+strlen(half_sep))
    {
      Console << "   ";  // spaces per hexa byte
      ascii += ' ';
    }
    Console << hex_to_str << ascii << separator;
  }

  Console << endl;
#endif
}
