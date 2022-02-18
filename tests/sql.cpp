#include <iostream>
#include <memory>
#include <algorithm>
#include <array>
#include <queue>
#include <condition_variable>
#include <random>
#include <sstream>
#include <iomanip>
#include <cstring>

#include "fastcgi++/sql/connection.hpp"
#include "fastcgi++/log.hpp"

#include <postgres.h>
#include <libpq-fe.h>
#include <catalog/pg_type.h>
#undef ERROR
#undef WARNING
#undef INFO

const unsigned totalQueries = 10000;
const unsigned maxQueriesSize = 1000;

using namespace Fastcgipp::SQL;

class TestQuery
{
private:
    static Connection connection;

    std::tuple<
        SMALLINT,
        BIGINT,
        TEXT,
        REAL,
        DOUBLE_PRECISION,
        BYTEA,
        WTEXT,
        TIMESTAMPTZ,
        INET,
        ARRAY<SMALLINT>,
        ARRAY<TEXT>,
        ARRAY<WTEXT>,
        BOOL,
        DATE> m_parameters;

    bool m_boolNull;

    std::shared_ptr<Results<INTEGER>> m_insertResult;

    std::shared_ptr<Results<
        SMALLINT,
        BIGINT,
        TEXT,
        REAL,
        DOUBLE_PRECISION,
        BYTEA,
        WTEXT,
        TIMESTAMPTZ,
        INET,
        ARRAY<SMALLINT>,
        ARRAY<TEXT>,
        ARRAY<WTEXT>,
        BOOL,
        DATE,
        WTEXT>> m_selectResults;

    std::shared_ptr<Results<>> m_deleteResult;

    std::function<void(Fastcgipp::Message)> m_callback;

    unsigned m_state;

    static std::map<unsigned, TestQuery> queries;

    static void callback(unsigned id, Fastcgipp::Message message);
    static std::queue<unsigned> queue;
    static std::mutex mutex;
    static std::condition_variable wake;

    static std::random_device device;
    static std::uniform_int_distribution<SMALLINT> smallIntDist;
    static std::uniform_int_distribution<INTEGER> integerDist;
    static std::uniform_int_distribution<BIGINT> bigIntDist;
    static std::normal_distribution<REAL> floatdist;
    static std::normal_distribution<DOUBLE_PRECISION> doubledist;
    static std::bernoulli_distribution boolDist;
    static const std::array<WTEXT, 6> wstrings;
    static const std::array<TEXT, 6> strings;
    static const std::array<BYTEA, 6> vectors;
    static const std::array<INET, 6> addresses;
    static const std::array<ARRAY<SMALLINT>, 6> int16Vectors;
    static const std::array<ARRAY<TEXT>, 6> stringVectors;
    static const std::array<ARRAY<WTEXT>, 6> wstringVectors;
    static std::uniform_int_distribution<unsigned> stringdist;

    bool handle();

public:
    TestQuery():
        m_state(0)
    {
    }

    static void init()
    {
        connection.init(
                "",
                "fastcgipp_test",
                "fastcgipp_test",
                "fastcgipp_test",
                8);
        connection.start();
    }

    static void stop()
    {
        connection.stop();
        connection.join();
    }

    static void handler();
};

Connection TestQuery::connection;
std::map<unsigned, TestQuery> TestQuery::queries;
std::queue<unsigned> TestQuery::queue;
std::mutex TestQuery::mutex;
std::condition_variable TestQuery::wake;

std::random_device TestQuery::device;
std::uniform_int_distribution<SMALLINT> TestQuery::smallIntDist(
        std::numeric_limits<SMALLINT>::min(),
        std::numeric_limits<SMALLINT>::max());
std::uniform_int_distribution<INTEGER> TestQuery::integerDist(
        std::numeric_limits<INTEGER>::min(),
        std::numeric_limits<INTEGER>::max());
std::uniform_int_distribution<BIGINT> TestQuery::bigIntDist(
        std::numeric_limits<BIGINT>::min(),
        std::numeric_limits<BIGINT>::max());
std::normal_distribution<REAL> TestQuery::floatdist(0, 1000);
std::normal_distribution<DOUBLE_PRECISION> TestQuery::doubledist(0, 10000);
std::bernoulli_distribution TestQuery::boolDist(0.5);
const std::array<WTEXT, 6> TestQuery::wstrings
{
    L"Hello World",
    L"Привет мир",
    L"Γεια σας κόσμο",
    L"世界您好",
    L"今日は世界",
    L"ᚺᛖᛚᛟ ᚹᛟᛉᛚᛞ"
};
const std::array<TEXT, 6> TestQuery::strings
{
    "Leviathan Wakes",
    "Caliban's War",
    "Abaddon's Gate",
    "Cibola Burn",
    "Nemesis Games",
    "Babylon's Ashes"
};
const std::array<BYTEA, 6> TestQuery::vectors
{{
    {'a', 'b', 'c', 'd', 'e', 'f'},
    {'b', 'c', 'd', 'e', 'f'},
    {'c', 'd', 'e', 'f'},
    {'d', 'e', 'f'},
    {'e', 'f'},
    {'f'}
}};
const std::array<INET, 6> TestQuery::addresses
{
    "cc22:4008:79a1:c178:5c5:882a:190d:7fbf",
    "ce9c:5116:7817::8d97:0:e755",
    "::ffff:179.124.131.145",
    "cc22:4008:79a1:c178:5c5:882a:190d:7fbf",
    "ce9c:5116:7817::8d97:0:e755",
    "::ffff:179.124.131.145"
};
const std::array<ARRAY<SMALLINT>, 6> TestQuery::int16Vectors
{{
    {16045, -10447, -30005, -28036, -10498, -3546},
    {28951, -27341, 31934, -18029, -10289},
    {-8362, 5513, -2999, 18684},
    {-488, -30159, 1865},
    {31456, 30510},
    {26529}
}};
const std::array<ARRAY<TEXT>, 6> TestQuery::stringVectors
{{
    {
        "The Fellowship of the Ring",
        "The Two Towers",
        "The Return of the King"
    },
    {
        "The Three-Body Problem",
        "The Dark Forest",
        "Death's End"
    },
    {
        "A New Hope",
        "The Empire Strikes Back",
        "Return of the Jedi"
    },
    {
        "Dragonflight",
        "Dragonquest",
        "The White Dragon"
    },
    {
        "The Fifth Season",
        "The Obelisk Gate",
        "The Stone Sky"
    },
    {
        "Leviathan Wakes",
        "Caliban's War",
        "Abaddon's Gate",
        "Cibola Burn",
        "Nemesis Games",
        "Babylon's Ashes"
    }
}};
const std::array<ARRAY<WTEXT>, 6> TestQuery::wstringVectors
{{
    {
        L"Братство Кольца",
        L"Две крепости",
        L"Возвращение короля"
    },
    {
        L"三体",
        L"黑暗森林",
        L"死神永生"
    },
    {
        L"A New Hope",
        L"The Empire Strikes Back",
        L"Return of the Jedi"
    },
    {
        L"Dragonflight",
        L"Dragonquest",
        L"The White Dragon"
    },
    {
        L"The Fifth Season",
        L"The Obelisk Gate",
        L"The Stone Sky"
    },
    {
        L"Leviathan Wakes",
        L"Caliban's War",
        L"Abaddon's Gate",
        L"Cibola Burn",
        L"Nemesis Games",
        L"Babylon's Ashes"
    }
}};
std::uniform_int_distribution<unsigned> TestQuery::stringdist(0,5);

void TestQuery::callback(unsigned id, Fastcgipp::Message message)
{
    std::lock_guard<std::mutex> lock(mutex);
    queue.push(id);
    wake.notify_one();
}

void TestQuery::handler()
{
    unsigned remaining = totalQueries;
    unsigned index=0;

    while(remaining)
    {
        while(index<totalQueries && queries.size()<maxQueriesSize)
        {
            if(queries.find(index) != queries.end())
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #1")
            auto& query = queries[index];
            query.m_callback = std::bind(
                    &TestQuery::callback,
                    index,
                    std::placeholders::_1);
            query.handle();
            ++index;
        }

        unsigned id;
        {
            std::unique_lock<std::mutex> lock(mutex);
            if(queue.empty())
                wake.wait(lock);
            id = queue.front();
            queue.pop();
        }

        const auto it = queries.find(id);
        if(it == queries.end())
            FAIL_LOG("Fastcgipp::SQL::Connection test fail #2")
        if(it->second.handle())
        {
            queries.erase(it);
            --remaining;
        }
    }
}

bool TestQuery::handle()
{
    switch(m_state)
    {
        case 0:
        {
            const auto now = std::chrono::system_clock::now();

            m_parameters = std::make_tuple(
                smallIntDist(device),
                bigIntDist(device),
                strings[stringdist(device)],
                floatdist(device),
                doubledist(device),
                vectors[stringdist(device)],
                wstrings[stringdist(device)],
                std::chrono::time_point_cast<TIMESTAMPTZ::duration>(now),
                addresses[stringdist(device)],
                int16Vectors[stringdist(device)],
                stringVectors[stringdist(device)],
                wstringVectors[stringdist(device)],
                boolDist(device),
                std::chrono::time_point_cast<std::chrono::days>(now));
            m_insertResult.reset(new Results<INTEGER>);

            m_boolNull = boolDist(device);

            Fastcgipp::SQL::Query query;
            query.statement = "INSERT INTO fastcgipp_test (zero, one, two, three, four, five, six, seven, eight, nine, ten, eleven, twelve, thirteen, fourteen) VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING zero;";
            query.parameters = make_Parameters(m_parameters);
            if(m_boolNull)
                query.parameters->setNull(12);
            query.results = m_insertResult;
            query.callback = m_callback;
            if(!connection.queue(query))
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #3")

            ++m_state;
            return false;
        }

        case 1:
        {
            if(m_insertResult->status() != Status::rowsOk || std::strcmp(statusString(m_insertResult->status()), "Rows OK"))
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #4: " << m_insertResult->errorMessage())
            if(m_insertResult->rows() != 1)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #5")
            if(m_insertResult->verify() != 0)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #6 " << m_insertResult->verify())

            const auto& row = m_insertResult->row(0);
            const INTEGER& id = std::get<0>(row);

            auto parameters = make_Parameters(id);
            m_selectResults.reset(new Results<
                    SMALLINT,
                    BIGINT,
                    TEXT,
                    REAL,
                    DOUBLE_PRECISION,
                    BYTEA,
                    WTEXT,
                    TIMESTAMPTZ,
                    INET,
                    ARRAY<SMALLINT>,
                    ARRAY<TEXT>,
                    ARRAY<WTEXT>,
                    BOOL,
                    DATE,
                    WTEXT>);

            Fastcgipp::SQL::Query query;
            query.statement = "SELECT one, two, three, four, five, six, seven, eight, nine, ten, eleven, twelve, thirteen, fourteen, zero::text || ' ' || one::text || ' ' || two::text || ' ' || three || ' ' || to_char(four, '9.999EEEE') || ' ' || to_char(five, '9.999EEEE') || ' ' || seven || ' ' || to_char(eight, 'YYYY-MM-DD HH24:MI:SS') || ' ' || nine || ' [,' || array_to_string(ten, ',') || '] [,' || array_to_string(eleven, ',') || '] ' || COALESCE(thirteen::TEXT, 'null') || ' ' || fourteen AS fifteen FROM fastcgipp_test WHERE zero=$1;";
            query.parameters = parameters;
            query.results = m_selectResults;
            query.callback = m_callback;
            if(!connection.queue(query))
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #7")

            ++m_state;
            return false;
        }

        case 2:
        {
            if(m_selectResults->status() != Status::rowsOk)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #8: " << m_selectResults->errorMessage())
            if(m_selectResults->rows() != 1)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #9")
            if(m_selectResults->verify() != 0)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #10: " << m_selectResults->verify())

            const auto& row = m_selectResults->row(0);

            if(m_selectResults->null(0, 0) || std::get<0>(row) != std::get<0>(m_parameters))
                FAIL_LOG("Check failed on column 0")
            if(m_selectResults->null(0, 1) || std::get<1>(row) != std::get<1>(m_parameters))
                FAIL_LOG("Check failed on column 1")
            if(m_selectResults->null(0, 2) || std::get<2>(row) != std::get<2>(m_parameters))
                FAIL_LOG("Check failed on column 2")
            if(m_selectResults->null(0, 3) || std::get<3>(row) != std::get<3>(m_parameters))
                FAIL_LOG("Check failed on column 3")
            if(m_selectResults->null(0, 4) || std::get<4>(row) != std::get<4>(m_parameters))
                FAIL_LOG("Check failed on column 4")
            if(m_selectResults->null(0, 5) || std::get<5>(row) != std::get<5>(m_parameters))
                FAIL_LOG("Check failed on column 5")
            if(m_selectResults->null(0, 6) || std::get<6>(row) != std::get<6>(m_parameters))
                FAIL_LOG("Check failed on column 6")
            if(m_selectResults->null(0, 7) || std::get<7>(row) != std::get<7>(m_parameters))
                FAIL_LOG("Check failed on column 7")
            if(m_selectResults->null(0, 8) || std::get<8>(row) != std::get<8>(m_parameters))
                FAIL_LOG("Check failed on column 8")
            if(m_selectResults->null(0, 9) || std::get<9>(row) != std::get<9>(m_parameters))
                FAIL_LOG("Check failed on column 9")
            if(m_selectResults->null(0, 10) || std::get<10>(row) != std::get<10>(m_parameters))
                FAIL_LOG("Check failed on column 10")
            if(m_selectResults->null(0, 11) || std::get<11>(row) != std::get<11>(m_parameters))
                FAIL_LOG("Check failed on column 11")
            if(m_boolNull)
            {
                if(!m_selectResults->null(0, 12))
                    FAIL_LOG("Null check failed on column 12")
            }
            else if(m_selectResults->null(0, 12) || std::get<12>(row) != std::get<12>(m_parameters))
                FAIL_LOG("Check failed on column 12")
            if(m_selectResults->null(0, 13) || std::get<13>(row) != std::get<13>(m_parameters))
                FAIL_LOG("Check failed on column 13")

            const std::time_t timeStamp = std::chrono::system_clock::to_time_t(
                    std::get<7>(m_parameters));

            std::wostringstream ss;
            ss  << std::get<0>(m_insertResult->row(0)) << ' '
                << std::get<0>(m_parameters) << ' '
                << std::get<1>(m_parameters) << ' '
                << std::get<2>(m_parameters).c_str() << ' '
                << std::scientific << std::setprecision(3)
                << (std::get<3>(m_parameters)>0?" ":"")
                << std::get<3>(m_parameters) << ' '
                << (std::get<4>(m_parameters)>0?" ":"")
                << std::get<4>(m_parameters) << ' '
                << std::get<6>(m_parameters)
                << std::put_time(std::localtime(&timeStamp), L" %Y-%m-%d %H:%M:%S ")
                << std::get<8>(m_parameters) << "/128 [";
            for(const auto& number: std::get<9>(m_parameters))
                ss << "," << number;
            ss << "] [";
            for(const auto& number: std::get<10>(m_parameters))
                ss << "," << number.c_str();
            ss  << "] ";
            if(m_boolNull)
                ss << "null";
            else
                ss << std::boolalpha << bool(std::get<12>(m_parameters));
            ss  << std::put_time(std::gmtime(&timeStamp), L" %Y-%m-%d");

            if(ss.str() != std::get<14>(row))
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #20 " << ss.str() << " vs " << std::get<14>(row))

            const auto& insertRow = m_insertResult->row(0);
            const INTEGER& id = std::get<0>(insertRow);

            auto parameters = make_Parameters(id);
            m_deleteResult.reset(new Results<>);

            Fastcgipp::SQL::Query query;
            query.statement = "DELETE FROM fastcgipp_test WHERE zero=$1;";
            query.parameters = parameters;
            query.results = m_deleteResult;
            query.callback = m_callback;
            if(!connection.queue(query))
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #21")

            ++m_state;
            return false;
        }

        case 3:
        {
            if(m_deleteResult->status() != Status::commandOk || std::strcmp(statusString(m_deleteResult->status()), "Command OK"))
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #22: " << m_deleteResult->errorMessage())
            if(m_deleteResult->rows() != 0)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #23")
            if(m_deleteResult->affectedRows() != 1)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #24")
            if(m_deleteResult->verify() != 0)
                FAIL_LOG("Fastcgipp::SQL::Connection test fail #25: " << m_deleteResult->verify())

            return true;
        }
    }
    return false;
}

int main()
{
    // Test the SQL parameters stuff
    {
        static const SMALLINT zero = -1413;
        static const INTEGER one = 123342945;
        static const BIGINT two = -123342945112312323;
        static const TEXT three = "This is a test!!34234";
        static const REAL four = -1656e-8;
        static const DOUBLE_PRECISION five = 2354e15;
        static const BYTEA six{'a', 'b', 'c', 'd', 'e', 'f'};

        static const WTEXT seven(L"インターネット");
        static const std::array<unsigned char, 21> properSeven =
        {
            0xe3, 0x82, 0xa4, 0xe3, 0x83, 0xb3, 0xe3, 0x82, 0xbf, 0xe3, 0x83,
            0xbc, 0xe3, 0x83, 0x8d, 0xe3, 0x83, 0x83, 0xe3, 0x83, 0x88
        };

        static const ARRAY<SMALLINT> eight{14662,5312,-5209,24755,-17290};
        static std::array<unsigned char, 50> properEight{
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x15,
            0x00, 0x00, 0x00, 0x05,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x02,
            0x39, 0x46,
            0x00, 0x00, 0x00, 0x02,
            0x14, 0xc0,
            0x00, 0x00, 0x00, 0x02,
            0xeb, 0xa7,
            0x00, 0x00, 0x00, 0x02,
            0x60, 0xb3,
            0x00, 0x00, 0x00, 0x02,
            0xbc, 0x76
        };
        static const ARRAY<TEXT> nine{
            "The Fellowship of the Ring",
            "The Two Towers",
            "The Return of the King"
        };
        static std::array<unsigned char, 94> properNine{
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x19,
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 26,
            'T','h','e',' ','F','e','l','l','o','w','s','h','i','p',' ','o','f',' ','t','h','e',' ','R','i','n','g',
            0x00, 0x00, 0x00, 14,
            'T','h','e',' ','T','w','o',' ','T','o','w','e','r','s',
            0x00, 0x00, 0x00, 22,
            'T','h','e',' ','R','e','t','u','r','n',' ','o','f',' ','t','h','e',' ','K','i','n','g'
        };
        static const ARRAY<WTEXT> ten{
            L"三体",
            L"黑暗森林",
            L"死神永生"
        };
        static std::array<unsigned char, 62> properTen{
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x19,
            0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 6,
            0xe4, 0xb8, 0x89, 0xe4, 0xbd, 0x93,
            0x00, 0x00, 0x00, 12,
            0xe9, 0xbb, 0x91, 0xe6, 0x9a, 0x97, 0xe6, 0xa3, 0xae, 0xe6, 0x9e, 0x97,
            0x00, 0x00, 0x00, 12,
            0xe6, 0xad, 0xbb, 0xe7, 0xa5, 0x9e, 0xe6, 0xb0, 0xb8, 0xe7, 0x94, 0x9f
        };
        static const BOOL eleven = false;
        static const BOOL twelve = true;

        auto data(make_Parameters(
                zero,
                one,
                two,
                three,
                four,
                five,
                six,
                seven,
                eight,
                nine,
                ten,
                eleven,
                twelve));
        for(unsigned i=0; i<eight.size(); ++i)
            if(eight[i] != std::get<8>(*data)[i])
                FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 8 []")
        for(unsigned i=0; i<nine.size(); ++i)
            if(nine[i] != std::get<9>(*data)[i])
                FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 9 []")
        for(unsigned i=0; i<ten.size(); ++i)
            if(ten[i] != std::get<10>(*data)[i])
                FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 10 []")
        std::shared_ptr<Parameters_base> base(data);
        data.reset();

        base->build();

        if(
                *(base->oids()+0) != INT2OID ||
                *(base->sizes()+0) != 2 ||
                Fastcgipp::BigEndian<SMALLINT>::read(
                    *(base->raws()+0)) != zero)
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 0")
        if(
                *(base->oids()+1) != INT4OID ||
                *(base->sizes()+1) != 4 ||
                Fastcgipp::BigEndian<INTEGER>::read(
                    *(base->raws()+1)) != one)
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 1")
        if(
                *(base->oids()+2) != INT8OID ||
                *(base->sizes()+2) != 8 ||
                Fastcgipp::BigEndian<BIGINT>::read(
                    *(base->raws()+2)) != two)
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 2")
        if(
                *(base->oids()+3) != TEXTOID ||
                *(base->sizes()+3) != three.size() ||
                !std::equal(
                    three.begin(),
                    three.end(),
                    *(base->raws()+3),
                    *(base->raws()+3)+*(base->sizes()+3)))
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 3")
        if(
                *(base->oids()+4) != FLOAT4OID ||
                *(base->sizes()+4) != 4 ||
                Fastcgipp::BigEndian<REAL>::read(
                    *(base->raws()+4)) != four)
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 4")
        if(
                *(base->oids()+5) != FLOAT8OID ||
                *(base->sizes()+5) != 8 ||
                Fastcgipp::BigEndian<DOUBLE_PRECISION>::read(
                    *(base->raws()+5)) != five)
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 5")
        if(
                *(base->oids()+6) != BYTEAOID ||
                *(base->sizes()+6) != six.size() ||
                !std::equal(six.cbegin(), six.cend(), *(base->raws()+6)))
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 6")
        if(
                *(base->oids()+7) != TEXTOID ||
                *(base->sizes()+7) != properSeven.size() ||
                !std::equal(
                    reinterpret_cast<const char*>(properSeven.begin()),
                    reinterpret_cast<const char*>(properSeven.end()),
                    *(base->raws()+7),
                    *(base->raws()+7)+*(base->sizes()+7)))
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 7")
        if(
                *(base->oids()+8) != INT2ARRAYOID ||
                *(base->sizes()+8) != properEight.size() ||
                !std::equal(
                    reinterpret_cast<const char*>(properEight.begin()),
                    reinterpret_cast<const char*>(properEight.end()),
                    *(base->raws()+8),
                    *(base->raws()+8)+*(base->sizes()+8)))
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 8 ")
        if(
                *(base->oids()+9) != TEXTARRAYOID ||
                *(base->sizes()+9) != properNine.size() ||
                !std::equal(
                    reinterpret_cast<const char*>(properNine.begin()),
                    reinterpret_cast<const char*>(properNine.end()),
                    *(base->raws()+9),
                    *(base->raws()+9)+*(base->sizes()+9)))
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 9 ")
        if(
                *(base->oids()+10) != TEXTARRAYOID ||
                *(base->sizes()+10) != properTen.size() ||
                !std::equal(
                    reinterpret_cast<const char*>(properTen.begin()),
                    reinterpret_cast<const char*>(properTen.end()),
                    *(base->raws()+10),
                    *(base->raws()+10)+*(base->sizes()+10)))
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 10 ")
        if(
                *(base->oids()+11) != BOOLOID ||
                *(base->sizes()+11) != 1 ||
                **(base->raws()+11) != eleven)
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 11")
        if(
                *(base->oids()+12) != BOOLOID ||
                *(base->sizes()+12) != 1 ||
                **(base->raws()+12) != twelve)
            FAIL_LOG("Fastcgipp::SQL::Parameters failed on column 12")
        for(
                const int* value = base->formats();
                value != base->formats() + base->size();
                ++value)
            if(*value != 1)
                FAIL_LOG("Fastcgipp::SQL::Parameters failed formats array")
    }

    // Test the SQL Connection
    {
        using namespace std::chrono_literals;
        TestQuery::init();
        std::this_thread::sleep_for(3s);
        TestQuery::handler();
        TestQuery::stop();
    }

    return 0;
}
