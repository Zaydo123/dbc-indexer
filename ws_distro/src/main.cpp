#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <cstdlib>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <limits>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = asio::ip::tcp;

// Sharded shared state: distributes sessions across shards and broadcasts in parallel
class shared_state {
 public:
  explicit shared_state(asio::io_context& ioc, std::size_t shard_count)
	: shards_(shard_count) {
	for (auto& sh : shards_) {
	  sh.strand = asio::make_strand(ioc);
	  sh.load = 0;
	}
  }

  // returns shard index assigned
  std::size_t join(std::shared_ptr<websocket::stream<beast::tcp_stream>> ws) {
	auto idx = pick_least_loaded();
	auto& sh = shards_[idx];
	{
	  std::lock_guard<std::mutex> lk(sh.mutex);
	  sh.sessions.push_back(ws);
	  sh.load++;
	}
	return idx;
  }

  void leave(std::size_t shard_idx, std::shared_ptr<websocket::stream<beast::tcp_stream>> ws) {
	if (shard_idx >= shards_.size()) return;
	auto& sh = shards_[shard_idx];
	std::lock_guard<std::mutex> lk(sh.mutex);
	sh.sessions.remove_if([&](const std::weak_ptr<websocket::stream<beast::tcp_stream>>& w){
	  auto s = w.lock();
	  return !s || s.get() == ws.get();
	});
	if (sh.load > 0) sh.load--;
  }

  void broadcast(const std::string& message) {
	// Post work to each shard's strand to parallelize across threads
	for (auto& sh : shards_) {
	  asio::dispatch(sh.strand, [this, &sh, message]() {
		std::lock_guard<std::mutex> lk(sh.mutex);
		for (auto it = sh.sessions.begin(); it != sh.sessions.end(); ) {
		  if (auto s = it->lock()) {
			beast::error_code ec;
			s->text(true);
			s->write(asio::buffer(message), ec); // sync write within strand
			if (ec) {
			  it = sh.sessions.erase(it);
			  if (sh.load > 0) sh.load--;
			  continue;
			}
			++it;
		  } else {
			it = sh.sessions.erase(it);
			if (sh.load > 0) sh.load--;
		  }
		}
	  });
	}
  }

 private:
  struct shard {
	asio::strand<asio::io_context::executor_type> strand{asio::io_context::executor_type{}};
	std::mutex mutex;
	std::list<std::weak_ptr<websocket::stream<beast::tcp_stream>>> sessions;
	std::atomic<std::size_t> load{0};
  };

  std::vector<shard> shards_;

  std::size_t pick_least_loaded() const {
	std::size_t best = 0;
	std::size_t best_load = std::numeric_limits<std::size_t>::max();
	for (std::size_t i = 0; i < shards_.size(); ++i) {
	  auto l = shards_[i].load.load(std::memory_order_relaxed);
	  if (l < best_load) { best = i; best_load = l; }
	}
	return best;
  }
};

// Handles a single WebSocket connection
class ws_session : public std::enable_shared_from_this<ws_session> {
 public:
  ws_session(tcp::socket&& socket, std::shared_ptr<shared_state> state)
	: ws_(std::make_shared<websocket::stream<beast::tcp_stream>>(std::move(socket))),
	  state_(std::move(state)) {}

  void run(http::request<http::string_body> req) {
	// Accept the WebSocket upgrade
	ws_->set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
	ws_->set_option(websocket::stream_base::decorator(
	  [](websocket::response_type& res) { res.set(http::field::server, "ws_distro"); }
	));

	beast::error_code ec;
	ws_->accept(req, ec);
	if (ec) return;

		shard_idx_ = state_->join(ws_);
	do_read();
  }

 private:
  void do_read() {
	auto self = shared_from_this();
		ws_->async_read(buffer_, [this, self](beast::error_code ec, std::size_t){
	  if (ec == websocket::error::closed || ec == asio::error::operation_aborted) {
		state_->leave(shard_idx_, ws_);
		return;
	  }
	  if (ec) {
		state_->leave(shard_idx_, ws_);
		return;
	  }
	  // Optional: echo back or ignore client messages
	  buffer_.clear();
	  do_read();
	});
  }

  std::shared_ptr<websocket::stream<beast::tcp_stream>> ws_;
  beast::flat_buffer buffer_;
  std::shared_ptr<shared_state> state_;
  std::size_t shard_idx_{0};
};

// HTTP session to handle /publish and upgrade to /ws
class http_session : public std::enable_shared_from_this<http_session> {
 public:
  http_session(tcp::socket&& socket, std::shared_ptr<shared_state> state)
	: stream_(std::move(socket)), state_(std::move(state)) {}

  void run() { do_read(); }

 private:
  void do_read() {
	req_ = {};
	stream_.expires_after(std::chrono::seconds(30));

	http::async_read(stream_, buffer_, req_,
	  beast::bind_front_handler(&http_session::on_read, shared_from_this()));
  }

  void on_read(beast::error_code ec, std::size_t) {
	if (ec == http::error::end_of_stream) return do_close();
	if (ec) return;

	// Handle WebSocket upgrade on /ws
	if (websocket::is_upgrade(req_)) {
	  if (req_.target() == "/ws") {
		std::make_shared<ws_session>(stream_.release_socket(), state_)->run(std::move(req_));
		return;
	  }
	}

	// Handle HTTP endpoints
	handle_request();
  }

  void handle_request() {
	http::response<http::string_body> res;
	res.version(req_.version());
	res.keep_alive(false);

	if (req_.method() == http::verb::post && req_.target() == "/publish") {
	  // Broadcast body to all websocket sessions
	  state_->broadcast(req_.body());
	  res.result(http::status::ok);
	  res.set(http::field::content_type, "application/json");
	  res.body() = std::string("{\"ok\":true}");
	} else if (req_.method() == http::verb::get && req_.target() == "/health") {
	  res.result(http::status::ok);
	  res.set(http::field::content_type, "application/json");
	  res.body() = std::string("{\"status\":\"up\"}");
	} else {
	  res.result(http::status::not_found);
	  res.set(http::field::content_type, "text/plain");
	  res.body() = std::string("Not found\n");
	}

	res.prepare_payload();

	auto self = shared_from_this();
	http::async_write(stream_, res, [this, self](beast::error_code ec, std::size_t){
	  stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
	});
  }

  void do_close() {
	beast::error_code ec;
	stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
  }

  beast::tcp_stream stream_;
  beast::flat_buffer buffer_;
  http::request<http::string_body> req_;
  std::shared_ptr<shared_state> state_;
};

// Accepts incoming connections and launches sessions
class listener : public std::enable_shared_from_this<listener> {
 public:
  listener(asio::io_context& ioc, tcp::endpoint endpoint, std::shared_ptr<shared_state> state)
	: ioc_(ioc), acceptor_(asio::make_strand(ioc)), state_(std::move(state)) {
	beast::error_code ec;

	acceptor_.open(endpoint.protocol(), ec);
	if (ec) throw beast::system_error(ec);
	acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
	if (ec) throw beast::system_error(ec);
	acceptor_.bind(endpoint, ec);
	if (ec) throw beast::system_error(ec);
	acceptor_.listen(asio::socket_base::max_listen_connections, ec);
	if (ec) throw beast::system_error(ec);
  }

  void run() { do_accept(); }

 private:
  void do_accept() {
	acceptor_.async_accept(asio::make_strand(ioc_),
	  beast::bind_front_handler(&listener::on_accept, shared_from_this()));
  }

  void on_accept(beast::error_code ec, tcp::socket socket) {
	if (!ec) {
	  std::make_shared<http_session>(std::move(socket), state_)->run();
	}
	do_accept();
  }

  asio::io_context& ioc_;
  tcp::acceptor acceptor_;
  std::shared_ptr<shared_state> state_;
};

int main(int argc, char* argv[]) {
  try {
	const char* host = std::getenv("WS_DISTRO_HOST");
	const char* port_env = std::getenv("WS_DISTRO_PORT");
	const char* threads_env = std::getenv("WS_DISTRO_MAX_THREADS");
	std::string host_str = host ? host : "0.0.0.0";
	unsigned short port = static_cast<unsigned short>(port_env ? std::atoi(port_env) : 8080);
	std::size_t max_threads = 0;
	if (threads_env) {
	  int t = std::atoi(threads_env);
	  max_threads = t > 0 ? static_cast<std::size_t>(t) : 1;
	} else {
	  unsigned hc = std::thread::hardware_concurrency();
	  max_threads = hc ? static_cast<std::size_t>(hc) : 1u;
	}

	asio::io_context ioc(static_cast<int>(max_threads));
	auto state = std::make_shared<shared_state>(ioc, max_threads);

	auto lst = std::make_shared<listener>(ioc, tcp::endpoint(asio::ip::make_address(host_str), port), state);
	lst->run();

	std::vector<std::thread> pool;
	pool.reserve(max_threads - 1);
	for (std::size_t i = 0; i + 1 < max_threads; ++i) {
	  pool.emplace_back([&ioc]{ ioc.run(); });
	}

	std::cout << "ws_distro listening on " << host_str << ":" << port
			  << " threads=" << max_threads << std::endl;
	ioc.run();
	for (auto& t : pool) t.join();
  } catch (const std::exception& e) {
	std::cerr << "Error: " << e.what() << std::endl;
	return 1;
  }
  return 0;
}
