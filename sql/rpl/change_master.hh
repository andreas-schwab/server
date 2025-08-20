/*
  Copyright (c) 2025 MariaDB

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin St, Fifth Floor, Boston, MA 02110-1335 USA.
*/

#include <optional>    // Storage type of @ref OptionalIntConfig
#include <my_global.h> // FN_REFLEN
#include <my_sys.h>    // IO_CACHE

/// Trilean: Enum alternative for "optional<bool>"
enum struct tril { NO, YES, DEFAULT= -1 };
/// Enum for @ref ChangeMaster::master_use_gtid
enum struct enum_master_use_gtid { NO, CURRENT_POS, SLAVE_POS, DEFAULT= -1 };

// `mariadbd` Options for @ref ChangeMaster::WithDefault
inline uint32_t master_connect_retry= 60;
inline float master_heartbeat_period= -1.0;
inline bool master_ssl= true;
inline const char *master_ssl_ca     = "";
inline const char *master_ssl_capath = "";
inline const char *master_ssl_cert   = "";
inline const char *master_ssl_crl    = "";
inline const char *master_ssl_crlpath= "";
inline const char *master_ssl_key    = "";
inline const char *master_ssl_cipher = "";
inline bool master_ssl_verify_server_cert= true;
inline auto master_use_gtid= enum_master_use_gtid::DEFAULT;
inline uint64_t master_retry_count= 100'000;

/// Persistence interface for an unspecified item
struct Persistent
{
  virtual ~Persistent()= default;
  /** set the value by reading a line from the IO and consume the `\n`
    @return `false` if successful or `true` if error
    @post is_default() is `false`
  */
  virtual bool load_from(IO_CACHE *file)= 0;
  /** write the *effective* value to the IO **without** a `\n`
    (The caller will separately determine how
     to represent using the default value.)
  */
  virtual void save_to(IO_CACHE *file)= 0;
  inline virtual bool is_default() { return false; }
  /// @return `true` if the item is mandatory and couldn't provide a default
  inline virtual bool set_default() { return true; }
  inline Persistent() { set_default(); }
};

struct ChangeMaster: Persistent
{
  /**
    @tparam T the interface type for using the configuration
    @tparam V the storage type for @ref value if different from `T`
  */
  template<typename T, typename V= T> struct Config: Persistent
  {
    V value;
    virtual operator T() = 0;
    inline V &operator=(T &value)
      { return this->value= value; }
    inline V &operator=(T &&value)
      { return this->value= std::forward<T>(value); }
    using Persistent::Persistent;
    inline Config(T  &args): Persistent(), value(args) {}
    inline Config(T &&args): Persistent(), value(std::forward<T>(args)) {}
  };

  /// for integers, signed or unsigned
  template<typename T, typename V= T> struct IntConfig: Config<T, V>
  {
    using Config<T, V>::operator=;
    virtual bool load_from(IO_CACHE *file) override;
    virtual void save_to(IO_CACHE *file) override;
  };
  /** for *optional* integers
    @see master_connect_retry
    @see master_retry_count
  */
  template<auto &mariadbd_option,
    typename I= std::remove_reference_t<decltype(mariadbd_option)>
  >
    struct OptionalIntConfig: IntConfig<I, std::optional<I>>
  {
    inline virtual bool is_default() override
      { return this->value.has_value(); }
    inline virtual bool set_default() override
    {
      this->value.reset();
      return true;
    }
    inline virtual operator I() override
      { return this->value.value_or(mariadbd_option); }
    using IntConfig<I, std::optional<I>>::operator=;
  };

  /// for SSL booleans
  template<bool &mariadbd_option> struct SSLBoolConfig: IntConfig<tril>
  {
    inline bool is_default() override
      { return this->value <= tril::DEFAULT; }
    inline bool set_default() override
    {
      this->value= tril::DEFAULT;
      return false;
    }
    inline operator tril() override
    {
      return this->is_default() ?
        static_cast<tril>(mariadbd_option) : this->value;
    }
    inline tril &operator=(bool value)
      { return *this= static_cast<tril>(value); }
  };
  /** for SSL paths
    They are @ref FN_REFLEN-sized null-terminated
    string buffers with `mariadbd` options as defaults.
  */
  template<const char *&mariadbd_option>
    struct SSLPathConfig: Config<const char *, char [FN_REFLEN]>
  {
    bool is_default() override;
    bool set_default() override;
    inline operator const char *() override { return this->value; }
    const char *operator=(const char *value);
    bool load_from(IO_CACHE *file) override;
    void save_to(IO_CACHE *file) override;
  };

  /// Singleton class for @ref master_heartbeat_period
  struct master_heartbeat_period_t: Config<float>
  {
    inline bool is_default() override { return this->value < 0; }
    inline bool set_default() override
    {
      this->value= -1.0;
      return false;
    }
    operator float() override;
    using Config<float>::operator=;
    bool load_from(IO_CACHE *file) override;
    void save_to(IO_CACHE *file) override;
  };
  /// Singleton class for @ref master_use_gtid
  struct master_use_gtid_t: IntConfig<enum_master_use_gtid>
  {
    bool gtid_supported= true;
    inline bool is_default() override
      { return this->value <= enum_master_use_gtid::DEFAULT; }
    inline bool set_default() override
    {
      this->value= enum_master_use_gtid::DEFAULT;
      return false;
    }
    operator enum_master_use_gtid() override;
    using IntConfig<enum_master_use_gtid>::operator=;
  };

  //// CHANGE MASTER entries; here in SHOW SLAVE STATUS order
  OptionalIntConfig<::master_connect_retry> master_connect_retry;
  master_heartbeat_period_t master_heartbeat_period;
  SSLBoolConfig<::master_ssl> master_ssl;
  SSLPathConfig<::master_ssl_ca> master_ssl_ca;
  SSLPathConfig<::master_ssl_capath> master_ssl_capath;
  SSLPathConfig<::master_ssl_cert> master_ssl_cert;
  SSLPathConfig<::master_ssl_crl> master_ssl_crl;
  SSLPathConfig<::master_ssl_crlpath> master_ssl_crlpath;
  SSLPathConfig<::master_ssl_key> master_ssl_key;
  SSLPathConfig<::master_ssl_cipher> master_ssl_cipher;
  SSLBoolConfig<::master_ssl_verify_server_cert> master_ssl_verify_server_cert;
  master_use_gtid_t master_use_gtid;
  OptionalIntConfig<::master_retry_count> master_retry_count;

  bool load_from(IO_CACHE *file) override;
  void save_to(IO_CACHE *file) override;
};
