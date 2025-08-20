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

#include "change_master.hh"
#include <string_view>   // Key type of @ref MASTER_INFO_MAP
#include <unordered_map> // Type of @ref MASTER_INFO_MAP
#include <charconv>      // std::from/to_chars
#include "../slave.h"    // init_str/floatvar_from_file

/// Replace an enum type with its underlying type or keep an int type as itself
template<typename I> using underlying_type= typename std::conditional_t<
  std::is_integral_v<I>,
  std::enable_if<true, I>, // C++20 std::type_identity
  std::underlying_type<I>
>::type;
/* Number of fully-utilized decimal digits plus
  * the partially-utilized digit (e.g., the 2's place in "2147483647")
  * The sign (:
*/
template<typename I> static constexpr size_t int_buf_size=
  std::numeric_limits<underlying_type<I>>::digits10 + 2;
static constexpr std::errc OK {};
/// zero and 64-bit capable version of init_intvar_from_file()
template<typename T, typename V>
bool ChangeMaster::IntConfig<T, V>::load_from(IO_CACHE *file)
{
  size_t size;
  underlying_type<T> value;
  // The `\0` is not required in std::from_chars(), but my_b_gets() includes it.
  char buf[int_buf_size<T> + 1];
  if ((size= my_b_gets(file, buf, int_buf_size<T> + 1)) &&
      std::from_chars(buf, &buf[size], value).ec != OK)
    return true;
  *this= static_cast<T>(value);
  return false;
}
template<typename T, typename V>
void ChangeMaster::IntConfig<T, V>::save_to(IO_CACHE *file)
{
  /*
    my_b_printf() uses a buffer too,
    so we might as well skip its format parsing step
  */
  char buf[int_buf_size<T>];
  std::to_chars_result to_chars_result= std::to_chars(
    buf, &buf[int_buf_size<T>],
    // C++23 std::to_underlying()
    static_cast<underlying_type<T>>(this->operator T())
  );
  DBUG_ASSERT(to_chars_result.ec == OK);
  my_b_write(file, (const uchar *)buf, int_buf_size<T>);
}

template<const char *&mariadbd_option>
bool ChangeMaster::SSLPathConfig<mariadbd_option>::is_default()
  { return !this->value[0] && this->value[1]; }
template<const char *&mariadbd_option>
bool ChangeMaster::SSLPathConfig<mariadbd_option>::set_default()
{
  this->value[0]= false;
  this->value[1]= true;
  return false;
}
template<const char *&mariadbd_option>
const char *
ChangeMaster::SSLPathConfig<mariadbd_option>::operator=(const char *value)
{
  if (value)
  {
    this->value[1]= false; // not default
    strmake_buf(this->value, value);
  }
  else // `nullptr`
    set_default();
  return this->value;
}
template<const char *&mariadbd_option>
bool ChangeMaster::SSLPathConfig<mariadbd_option>::load_from(IO_CACHE *file)
{
  this->value[1]= false; // not default
  return init_strvar_from_file(this->value, FN_REFLEN, file, nullptr);
}
template<const char *&mariadbd_option>
void ChangeMaster::SSLPathConfig<mariadbd_option>::save_to(IO_CACHE *file)
  { my_b_write(file,
    (const uchar *)(this->operator const char *()), strlen(*this)
  ); }

ChangeMaster::master_heartbeat_period_t::operator float()
{
  return is_default() ? (
    ::master_heartbeat_period < 0 ?
      slave_net_timeout/2.0 : ::master_heartbeat_period
    ) : this->value;
}
bool ChangeMaster::master_heartbeat_period_t::load_from(IO_CACHE *file)
  { return init_floatvar_from_file(&this->value, file, 0); }
void ChangeMaster::master_heartbeat_period_t::save_to(IO_CACHE *file)
{
  //TODO: `master_heartbeat_period` should at most be a `DECIMAL(10, 3)`.
  char buf[FLOATING_POINT_BUFFER];
  size_t size= my_fcvt(this->operator float(), 3, buf, nullptr);
  my_b_write(file, (const uchar *)buf, size);
}

ChangeMaster::master_use_gtid_t::operator enum_master_use_gtid()
{
  return this->is_default() ? (
    ::master_use_gtid > enum_master_use_gtid::DEFAULT ?
      ::master_use_gtid : gtid_supported ?
        enum_master_use_gtid::SLAVE_POS : enum_master_use_gtid::NO
    ) : this->value;
}

/**
  std::mem_fn()-like replacement for
  [member pointer upcasting](https://wg21.link/P0149R3)
*/
struct mem_fn
{
  std::function<Persistent &(ChangeMaster *connection)> get;
  template<typename M> mem_fn(M ChangeMaster::* pm):
    get([pm](ChangeMaster *self) -> Persistent & { return self->*pm; }) {}
};
/// An iterable for the `key=value` section of `@@master_info_file`
// C++ default allocator to match that `mysql_execute_command()` uses `new`
static const std::unordered_map<std::string_view, mem_fn> MASTER_INFO_MAP({
  /* MySQL line-based section
    ChangeMaster::save_to() only annotates whether they are `DEFAULT`.
  */
  {"connect_retry"         , &ChangeMaster::master_connect_retry         },
  {"ssl"                   , &ChangeMaster::master_ssl                   },
  {"ssl_ca"                , &ChangeMaster::master_ssl_ca                },
  {"ssl_capath"            , &ChangeMaster::master_ssl_capath            },
  {"ssl_cert"              , &ChangeMaster::master_ssl_cert              },
  {"ssl_cipher"            , &ChangeMaster::master_ssl_cipher            },
  {"ssl_key"               , &ChangeMaster::master_ssl_key               },
  {"ssl_crl"               , &ChangeMaster::master_ssl_crl               },
  {"ssl_crlpath"           , &ChangeMaster::master_ssl_crlpath           },
  {"ssl_verify_server_cert", &ChangeMaster::master_ssl_verify_server_cert},
  {"heartbeat_period"      , &ChangeMaster::master_heartbeat_period      },
  {"retry_count"           , &ChangeMaster::master_retry_count           },
  /* The actual MariaDB `key=value` section
    For backward compatibility,
    keys should match the corresponding old property name in @ref Master_info.
  */
  {"using_gtid",             &ChangeMaster::master_use_gtid              }
});

bool ChangeMaster::load_from(IO_CACHE *file) { return true; }
/// Currently only appends the `file` with those in @ref MASTER_INFO_MAP
void ChangeMaster::save_to(IO_CACHE *file)
{
  /*
    For the current set of configs, only ChangeMaster::master_use_gtid
    is always saved as a `key=value` pair.
  */
  if (!master_use_gtid.is_default())
  {
    my_b_write(file, (const uchar *)"using_gtid=", sizeof("using_gtid"));
    master_use_gtid.save_to(file);
  }
  for (auto &[key, member]: MASTER_INFO_MAP)
  {
    // The others only need to save a key to mark that they're set to `DEFAULT`.
    if (member.get(this).is_default())
      my_b_write(file, (const uchar *)key.data(), key.size());
  }
}
