/*!
 * @file       sqlTraits.hpp
 * @brief      Defines SQL type traits
 * @author     Eddie Carle &lt;eddie@isatec.ca&gt;
 * @date       February 16, 2022
 * @copyright  Copyright &copy; 2022 Eddie Carle. This project is released under
 *             the GNU Lesser General Public License Version 3.
 */

/*******************************************************************************
* Copyright (C) 2022 Eddie Carle [eddie@isatec.ca]                             *
*                                                                              *
* This file is part of fastcgi++.                                              *
*                                                                              *
* fastcgi++ is free software: you can redistribute it and/or modify it under   *
* the terms of the GNU Lesser General Public License as  published by the Free *
* Software Foundation, either version 3 of the License, or (at your option)    *
* any later version.                                                           *
*                                                                              *
* fastcgi++ is distributed in the hope that it will be useful, but WITHOUT ANY *
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    *
* FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for     *
* more details.                                                                *
*                                                                              *
* You should have received a copy of the GNU Lesser General Public License     *
* along with fastcgi++.  If not, see <http://www.gnu.org/licenses/>.           *
*******************************************************************************/

#ifndef FASTCGIPP_SQL_TRAITS_HPP
#define FASTCGIPP_SQL_TRAITS_HPP

#include <string>
#include <postgres.h>
#include <libpq-fe.h>
#include <catalog/pg_type.h>
#include <utils/inet.h>
#undef WARNING
#undef INFO
#undef ERROR

#include "fastcgi++/sql/types.hpp"

//! Topmost namespace for the fastcgi++ library
namespace Fastcgipp
{
    //! Contains all fastcgi++ %SQL facilities
    namespace SQL
    {
        template<typename T> struct Traits {};
        template<> struct Traits<BOOL>
        {
            static constexpr unsigned oid = BOOLOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 1;
            }
        };
        template<> struct Traits<SMALLINT>
        {
            static constexpr unsigned oid = INT2OID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 2;
            }
        };
        template<> struct Traits<INTEGER>
        {
            static constexpr unsigned oid = INT4OID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 4;
            }
        };
        template<> struct Traits<BIGINT>
        {
            static constexpr unsigned oid = INT8OID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 8;
            }
        };
        template<> struct Traits<REAL>
        {
            static constexpr unsigned oid = FLOAT4OID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 4;
            }
        };
        template<> struct Traits<DOUBLE_PRECISION>
        {
            static constexpr unsigned oid = FLOAT8OID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 8;
            }
        };
        template<> struct Traits<TEXT>
        {
            static constexpr unsigned oid = TEXTOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<WTEXT>
        {
            static constexpr unsigned oid = TEXTOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<TIMESTAMPTZ>
        {
            static constexpr unsigned oid = TIMESTAMPTZOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 8;
            }
        };
        template<> struct Traits<DATE>
        {
            static constexpr unsigned oid = DATEOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                const auto size = PQfsize(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid && size == 4;
            }
        };
        template<> struct Traits<INET>
        {
            static constexpr unsigned oid = INETOID;
            static constexpr char addressFamily = PGSQL_AF_INET6;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<BYTEA>
        {
            static constexpr unsigned oid = BYTEAOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<ARRAY<SMALLINT>>
        {
            static constexpr unsigned oid = INT2ARRAYOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<ARRAY<INTEGER>>
        {
            static constexpr unsigned oid = INT4ARRAYOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<ARRAY<BIGINT>>
        {
            static constexpr unsigned oid = INT8ARRAYOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<ARRAY<REAL>>
        {
            static constexpr unsigned oid = FLOAT4ARRAYOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<ARRAY<DOUBLE_PRECISION>>
        {
            static constexpr unsigned oid = FLOAT8ARRAYOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<ARRAY<TEXT>>
        {
            static constexpr unsigned oid = TEXTARRAYOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
        template<> struct Traits<ARRAY<WTEXT>>
        {
            static constexpr unsigned oid = TEXTARRAYOID;
            static bool verifyType(const void* result, int column)
            {
                const Oid type = PQftype(
                        reinterpret_cast<const PGresult*>(result),
                        column);
                return type == oid;
            }
        };
    }
}

#endif
