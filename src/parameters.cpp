/*!
 * @file       parameters.cpp
 * @brief      Defines SQL parameters types
 * @author     Eddie Carle &lt;eddie@isatec.ca&gt;
 * @date       February 18, 2022
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

#include "sqlTraits.hpp"
#include "fastcgi++/sql/parameters.hpp"
#include "fastcgi++/log.hpp"

#include <locale>
#include <codecvt>

using namespace Fastcgipp::SQL;

TEXT Parameter<WTEXT>::convert(const WTEXT& x)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> converter;
    try
    {
        return converter.to_bytes(x);
    }
    catch(const std::range_error& e)
    {
        WARNING_LOG("Error in code conversion to utf8 in SQL parameter")
    }
    return TEXT();
}

const unsigned Parameter<BOOL>::oid = Traits<BOOL>::oid;
const unsigned Parameter<SMALLINT>::oid = Traits<SMALLINT>::oid;
const unsigned Parameter<INTEGER>::oid = Traits<INTEGER>::oid;
const unsigned Parameter<BIGINT>::oid = Traits<BIGINT>::oid;
const unsigned Parameter<REAL>::oid = Traits<REAL>::oid;
const unsigned Parameter<DOUBLE_PRECISION>::oid = Traits<DOUBLE_PRECISION>::oid;
const unsigned Parameter<TEXT>::oid = Traits<TEXT>::oid;
const unsigned Parameter<WTEXT>::oid = Traits<WTEXT>::oid;
const unsigned Parameter<BYTEA>::oid = Traits<BYTEA>::oid;
const unsigned Parameter<TIMESTAMPTZ>::oid = Traits<TIMESTAMPTZ>::oid;
const unsigned Parameter<DATE>::oid = Traits<DATE>::oid;
const unsigned Parameter<INET>::oid = Traits<INET>::oid;
const char Parameter<INET>::addressFamily = Traits<INET>::addressFamily;
template<typename Numeric>
const unsigned Parameter<ARRAY<Numeric>>::oid = Traits<ARRAY<Numeric>>::oid;
const unsigned Parameter<ARRAY<TEXT>>::oid = Traits<ARRAY<TEXT>>::oid;

template<typename Numeric> void Parameter<ARRAY<Numeric>>::resize(
        const unsigned size)
{
    m_size = sizeof(ARRAY_SIZE)*(5+size) + size*sizeof(Numeric);
    m_data.reset(new char[m_size]);

    ARRAY_SIZE& ndim(*reinterpret_cast<ARRAY_SIZE*>(
                m_data.get()+0*sizeof(ARRAY_SIZE)));
    ARRAY_SIZE& hasNull(*reinterpret_cast<ARRAY_SIZE*>(
                m_data.get()+1*sizeof(ARRAY_SIZE)));
    ARRAY_SIZE& elementType(*reinterpret_cast<ARRAY_SIZE*>(
                m_data.get()+2*sizeof(ARRAY_SIZE)));
    ARRAY_SIZE& dim(*reinterpret_cast<ARRAY_SIZE*>(
                m_data.get()+3*sizeof(ARRAY_SIZE)));
    ARRAY_SIZE& lBound(*reinterpret_cast<ARRAY_SIZE*>(
                m_data.get()+4*sizeof(ARRAY_SIZE)));

    ndim = 1;
    hasNull = 0;
    elementType = Traits<Numeric>::oid;
    dim = size;
    lBound = 1;
}

template<typename Numeric>
Parameter<ARRAY<Numeric>>& Parameter<ARRAY<Numeric>>::operator=(
        const ARRAY<Numeric>& x)
{
    resize(x.size());

    for(unsigned i=0; i < x.size(); ++i)
    {
        char* ptr = m_data.get() + 5*sizeof(ARRAY_SIZE)
            + i*(sizeof(ARRAY_SIZE) + sizeof(Numeric));

        ARRAY_SIZE& length(*reinterpret_cast<ARRAY_SIZE*>(ptr));
        BigEndian<Numeric>& value(*reinterpret_cast<BigEndian<Numeric>*>(
                    ptr+sizeof(ARRAY_SIZE)));

        length = sizeof(Numeric);
        value = x[i];
    }

    return *this;
}

template class Parameter<ARRAY<SMALLINT>>;
template class Parameter<ARRAY<INTEGER>>;
template class Parameter<ARRAY<BIGINT>>;
template class Parameter<ARRAY<REAL>>;
template class Parameter<ARRAY<DOUBLE_PRECISION>>;

void Parameter<ARRAY<TEXT>>::assign(const ARRAY<TEXT>& x)
{
    // Allocate the space
    {
        unsigned dataSize = 0;
        for(const auto& string: x)
            dataSize += string.size();
        m_size = sizeof(ARRAY_SIZE)*(5+x.size()) + dataSize;
        m_data.reset(new char[m_size]);
        ARRAY_SIZE& ndim(*reinterpret_cast<ARRAY_SIZE*>(
                    m_data.get()+0*sizeof(ARRAY_SIZE)));
        ARRAY_SIZE& hasNull(*reinterpret_cast<ARRAY_SIZE*>(
                    m_data.get()+1*sizeof(ARRAY_SIZE)));
        ARRAY_SIZE& elementType(*reinterpret_cast<ARRAY_SIZE*>(
                    m_data.get()+2*sizeof(ARRAY_SIZE)));
        ARRAY_SIZE& dim(*reinterpret_cast<ARRAY_SIZE*>(
                    m_data.get()+3*sizeof(ARRAY_SIZE)));
        ARRAY_SIZE& lBound(*reinterpret_cast<ARRAY_SIZE*>(
                    m_data.get()+4*sizeof(ARRAY_SIZE)));

        ndim = 1;
        hasNull = 0;
        elementType = Traits<TEXT>::oid;
        dim = x.size();
        lBound = 1;
    }

    char* ptr = m_data.get() + 5*sizeof(ARRAY_SIZE);
    for(const auto& string: x)
    {
        ARRAY_SIZE& length(
                *reinterpret_cast<ARRAY_SIZE*>(ptr));
        length = string.size();
        ptr = std::copy(string.begin(), string.end(), ptr+sizeof(ARRAY_SIZE));
    }
}

TEXT Parameter<ARRAY<TEXT>>::operator[](const unsigned x) const
{
    const char* ptr = m_data.get() + 5*sizeof(ARRAY_SIZE);
    unsigned i = 0;
    while(true)
    {
        const int32_t length(*reinterpret_cast<const ARRAY_SIZE*>(ptr));
        ptr += sizeof(ARRAY_SIZE);
        if(i++ == x)
            return TEXT(ptr, length);
        ptr += length;
    }
}

ARRAY<TEXT> Parameter<ARRAY<WTEXT>>::convert(const ARRAY<WTEXT>& x)
{
    ARRAY<TEXT> result;
    result.reserve(x.size());

    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> converter;
    try
    {
        for(const auto& string: x)
            result.emplace_back(std::move(converter.to_bytes(string)));
    }
    catch(const std::range_error& e)
    {
        WARNING_LOG("Error in array code conversion to utf8 in SQL parameter")
    }
    return result;
}

WTEXT Parameter<ARRAY<WTEXT>>::convert(const TEXT& x)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> converter;
    try
    {
        return converter.from_bytes(x);
    }
    catch(const std::range_error& e)
    {
        WARNING_LOG("Error in array code conversion from utf8 in SQL parameter")
    }
    return WTEXT();
}
