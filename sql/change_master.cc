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

#include "change_master.ccm"
#include <string_view>   // Key type of @ref MASTER_INFO_MAP
#include <unordered_map> // Type of @ref MASTER_INFO_MAP
#include <unordered_set> // seen set in `load_from()`

ChangeMaster::master_heartbeat_period_t::operator float()
{
  return is_default() ? (
    ::master_heartbeat_period < 0 ?
      slave_net_timeout/2.0 : ::master_heartbeat_period
    ) : period;
}
bool ChangeMaster::master_heartbeat_period_t::load_from(IO_CACHE *file)
  { return init_floatvar_from_file(&period, file, 0); }
void ChangeMaster::master_heartbeat_period_t::save_to(IO_CACHE *file)
{
  //TODO: `master_heartbeat_period` should at most be a `DECIMAL(10, 3)`.
  char buf[FLOATING_POINT_BUFFER];
  size_t size= my_fcvt(*this, 3, buf, nullptr);
  my_b_write(file, (const uchar *)buf, size);
}

ChangeMaster::master_use_gtid_t::operator enum_master_use_gtid()
{
  return is_default() ? (
    (::master_use_gtid < OPT_MASTER_USE_GTID_DEFAULT) ?
      static_cast<enum_master_use_gtid>(::master_use_gtid) :
      gtid_supported ?
        enum_master_use_gtid::SLAVE_POS : enum_master_use_gtid::NO
  ) : mode;
}
/// Replace this enum type with the integral type under its trench coat
using use_gtid_t= std::underlying_type_t<enum_master_use_gtid>;
bool ChangeMaster::master_use_gtid_t::load_from(IO_CACHE *file)
{
  use_gtid_t value;
  if (from_chars(file, value) ||
      value > static_cast<use_gtid_t>(enum_master_use_gtid::SLAVE_POS) ||
      value < static_cast<use_gtid_t>(enum_master_use_gtid::NO))
    return true;
  *this= static_cast<enum_master_use_gtid>(value);
  return false;
}
void ChangeMaster::master_use_gtid_t::save_to(IO_CACHE *file)
{
  return to_chars(file, static_cast<unsigned char>(
    static_cast<enum_master_use_gtid>(*this))
  );
}

/**
  Guard agaist extra left-overs at the end of file,
  in case a later update causes the file to shrink compared to earlier contents
*/
static constexpr const char END_MARKER[]= "END_MARKER";

/**
  std::mem_fn()-like replacement for
  [member pointer upcasting](https://wg21.link/P0149R3)
*/
struct mem_fn
{
  std::function<Persistent &(ChangeMaster *connection)> get;
  mem_fn(): get() {}
  template<typename M> mem_fn(M ChangeMaster::* pm):
    get([pm](ChangeMaster *self) -> Persistent & { return self->*pm; }) {}
};
/// An iterable for the `key=value` section of `@@master_info_file`
// C++ default allocator to match that `mysql_execute_command()` uses `new`
static const std::unordered_map<std::string_view, mem_fn> MASTER_INFO_MAP({
  /* MySQL line-based section:
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
  /* The actual MariaDB `key=value` section:
    For backward compatibility,
    keys should match the corresponding old property name in @ref Master_info.
  */
  {"using_gtid",        &ChangeMaster::master_use_gtid  },
  {"do_domain_ids",     &ChangeMaster::do_domain_ids    },
  {"ignore_domain_ids", &ChangeMaster::ignore_domain_ids},
  {END_MARKER, mem_fn()}
});

ChangeMaster::ChangeMaster(DYNAMIC_ARRAY m_domain_ids[2]):
  do_domain_ids(&m_domain_ids[0]), ignore_domain_ids(&m_domain_ids[1])
{
  for(auto &[_, member]: MASTER_INFO_MAP)
    if (static_cast<bool>(member.get))
      member.get(this).set_default();
}

/// Repurpose the trailing `\0` spot to prepare for the `=` or `\n`
static constexpr size_t MAX_KEY_SIZE= sizeof("ssl_verify_server_cert");
static const decltype(MASTER_INFO_MAP)::const_iterator KEY_NOT_FOUND=
  MASTER_INFO_MAP.cend(); // `constexpr` in C++26

bool ChangeMaster::load_from(IO_CACHE *file)
{
  /*
    10.0 does not have the `END_MARKER` before any left-overs at the
    end of the file. So ignore any but the first occurrence of a key.
  */
  std::unordered_set<const char *> seen{};
  /* Parse additional `key=value` lines:
    The "value" can then be parsed individually after consuming the`key=`.
  */
  while (true)
  {
    bool found_equal= false;
    char key[MAX_KEY_SIZE];
    // Modified from the old `read_mi_key_from_file()`
    for (size_t i=0; i < MAX_KEY_SIZE; ++i)
    {
      switch (int c= my_b_get(file)) {
      case my_b_EOF:
        return true;
      case '=':
        found_equal= true;
      [[fallthrough]];
      case '\n':
      {
        decltype(MASTER_INFO_MAP)::const_iterator found_kv=
          MASTER_INFO_MAP.find(std::string_view(
            key,
            i // size = exclusive end index of the string
          ));
        // The "unknown" lines would be ignored to facilitate downgrades.
        if (found_kv != KEY_NOT_FOUND)
        {
          const char *key= found_kv->first.data();
          if (key == END_MARKER)
            return false;
          else if (seen.insert(key).second) // if no previous insertion
          {
            Persistent &config= found_kv->second.get(this);
            /*
              Keys that support saving the `DEFAULT` will represent the
              `DEFAULT` by omitting the `=value` part; though here we allow
              them to include the `=value` part for non-`DEFAULT` too.
            */
            if (found_equal ? config.load_from(file) : config.set_default())
              sql_print_error("Failed to initialize master info %s", key);
          }
        }
        goto break_for;
      }
      default:
        key[i]= c;
      }
    }
    break_for:;
  }
}

void ChangeMaster::save_to(IO_CACHE *file)
{
  /*
    For the current set of configs,
    only three are always saved as a `key=value` pair.
  */
  if (!master_use_gtid.is_default())
  {
    my_b_write(file, (const uchar *)"using_gtid=", sizeof("using_gtid"));
    master_use_gtid.save_to(file);
    my_b_write_byte(file, '\n');
  }
  if (!do_domain_ids.is_default())
  {
    my_b_write(file, (const uchar *)"do_domain_ids=", sizeof("do_domain_ids"));
    do_domain_ids.save_to(file);
    my_b_write_byte(file, '\n');
  }
  if (!ignore_domain_ids.is_default())
  {
    my_b_write(file, (const uchar *)"ignore_domain_ids=",
                     sizeof("ignore_domain_ids"));
    ignore_domain_ids.save_to(file);
    my_b_write_byte(file, '\n');
  }
  for (auto &[key, member]: MASTER_INFO_MAP)
  {
    // The others only need to save a key to mark that they're set to `DEFAULT`.
    if (static_cast<bool>(member.get) && member.get(this).is_default())
    {
      my_b_write(file, (const uchar *)key.data(), key.size());
      my_b_write_byte(file, '\n');
    }
  }
  my_b_write(file, (const uchar *)END_MARKER,
             sizeof(END_MARKER) - /* the '\0' */ 1);
  my_b_write_byte(file, '\n');
}
