module CodeGov

filter!(isequal(joinpath(homedir(), ".julia")), DEPOT_PATH)
using Pkg
Pkg.activate(@__DIR__)
using HTTP: request
using JSON3: JSON3
using JSONTables: jsontable
using DataFrames: DataFrame
using LibPQ: Connection, execute, ZonedDateTime, DateFormat, load!
ENV["GSA_KEY"] = ""
const GSA_KEY = ENV["GSA_KEY"]
foreach(println, ENV)
"""
    find_all_agencies(GSA_KEY::AbstractString = ENV["GSA_KEY"])

Return a list of all agencies in the Code.gov API.
"""
function find_all_agencies(GSA_KEY::AbstractString = ENV["GSA_KEY"])
    response = request("GET",
                       "https://api.code.gov/agencies?size=200",
                       ["Content-Type" => "application/json",
                        "x-api-key" => GSA_KEY])
    json = JSON3.read(response.body)
    total = json.total
    agencies = jsontable(json.agencies)
    @assert (length(agencies) == total) && total < 200
    agencies
end

agencies = find_all_agencies()

function parse_repository(obj)
    (repoid = obj.repoID,
     score = obj.score,
     agency = obj.agency.acronym,
     status = haskey(obj, :status) ? obj.status : missing,
     vcs = haskey(obj, :vcs) ? obj.vcs : missing,
     repository_url = obj.repositoryURL,
     os = haskey(obj, :targetOperatingSystems) ? obj.targetOperatingSystems : missing,
     name = obj.name,
     version = haskey(obj, :version) ? obj.version : missing,
     description = obj.description,
     tags = haskey(obj, :tags) ? obj.tags : Vector{String}(),
     license = haskey(obj.permissions, :licenses) ? obj.permissions.licenses : missing,
     usage = haskey(obj.permissions, :usageType) ? obj.permissions.usageType : missing,
     organization = haskey(obj, :organization) ? obj.organization : missing,
     languages = haskey(obj, :languages) ? (isa(obj.languages, AbstractString) ? [ obj.languages ] : string.(obj.languages))  : Vector{String}(),
     laborhours = haskey(obj, :laborHours) ? obj.laborHours : missing,
     homepage_url = haskey(obj, :homepageURL) ? obj.homepageURL : missing,
     download_url = haskey(obj, :downloadURL) ? obj.downloadURL : missing,
     created = haskey(obj, :date) ? (haskey(obj.date, :created) ? obj.date.created : missing) : missing,
     last_modified = haskey(obj, :date) ? (haskey(obj.date, :lastModified) ? obj.date.lastModified : missing) : missing,
     metadata_lastupdated = haskey(obj, :date) ? (haskey(obj.date, :metadataLastUpdated) ? obj.date.metadataLastUpdated : missing) : missing,
     contact_name = haskey(obj.contact, :name) ? obj.contact.name : missing,
     contact_url = haskey(obj.contact, :URL) ? obj.contact.URL : missing,
     contact_email = obj.contact.email,
     contact_twitter = haskey(obj.contact, :twitter) ? obj.contact.twitter : missing,
     
     )
end
for (idx, repo) in enumerate(repos)
    println(idx)
    parse_repository(repo)
    # repos[784]
end
for (idx, repo) in enumerate(json.repos)
    println(idx)
    parse_repository(repo)
end
chk = DataFrame(parse_repository(repo) for repo in repos)
unique(vcat(repos, chk2)[!,:languages])
chk2 = DataFrame(parse_repository(repo) for repo in json2.repos)
chk3 = DataFrame(parse_repository(repo) for repo in json3.repos)
chk4 = DataFrame(parse_repository(repo) for repo in json4.repos)
chk5 = DataFrame(parse_repository(repo) for repo in json5.repos)
vcat(repos, chk2, chk3, chk4, chk5)[!,:languages]
x = [1, missing]
ifelse.(ismissing.(x), missing, sqrt.(x))
(x -> ismissing(x) ? missing : ZonedDateTime(x)).(repos[!,:created])
x = DataFrame(x = [-1, missing], y = [Missing, "2019-06-05T00:00:00.000Z"])
ifelse.(ismissing.(x.x), missing, sqrt.(x.x))
ifelse.(ismissing.(x.y), missing, ZonedDateTime.(x.y))
sqrt(missing)
x = -1:1
ifelse.(x .≥ 0, sqrt.(x), 0)
broadcast(x -> x ≥ 0 ? sqrt(x) : 0, x)

repos[!, :created] = broadcast(x -> ismissing(x) ? missing : ZonedDateTime(x), repos[!, :created])
repos[!, :last_modified] = broadcast(x -> ismissing(x) ? missing : ZonedDateTime(x), repos[!, :last_modified])
repos[!, :metadata_lastupdated] = broadcast(x -> ismissing(x) ? missing : ZonedDateTime(x), repos[!, :metadata_lastupdated])
make_text_vec(obj) = string("'{", join(("'$x'" for x in obj), ','), "}'::text[]")
make_text_vec(obj::Missing) = missing
repos[ismissing.(repos.tags), :tags] .= "'{}'::text[]"
repos[!, :tags] = make_text_vec.(repos[!,:tags])
repos[!, :tags] = replace.(SubString.(repos[!, :tags], 2), "}'" => "}")
repos[!, :languages] = make_text_vec.(repos[!,:languages])
repos[!, :languages] = replace.(SubString.(repos[!, :languages], 2), "}'" => "}")
repos[!, :tags] = replace.(repos[!, :tags], "}::text[]" => "}")
repos[!, :languages] = replace.(repos[!, :languages], "}::text[]" => "}")

replace.(SubString.(repos[!, :tags], 2), "}'" => "}")

conn = Connection("""
                  host = $(get(ENV, "PGHOST", ""))
                  dbname = sdad
                  user = $(get(ENV, "DB_USR", ""))
                  password = $(get(ENV, "DB_PWD", ""))
                  """);

execute(conn, "BEGIN;")
load!(repos, conn, "INSERT INTO codegov.repos VALUES ($(join(("\$$i" for i in 1:23), ',')));")
execute(conn, "COMMIT;")

ifelse.(ismissing.(repos[!,:created]), missing, ZonedDateTime.(skipmissing(repos[!,:created])))

ZonedDateTime("2019-06-05T00:00:00.000Z"), DateFormat("yyyy-mm-ddTHH:MM:SS.sssz"))
append!(deepcopy(repos), chk2)
foreach(col -> println(eltype(col)), eachcol(repos[!,:languages]))

foreach((idx, repo) -> (println(idx); parse_repository(repo)), enumerate(repos))
"""
{

  "measurementType": {
    "type": "object",
    "properties": {
      "method": {
        "type": "keyword"
      },
      "ifOther": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      }
    }
  },
  "events": {
    "type": "text",
    "fields": {
      "keyword": {
        "type": "keyword",
        "normalizer": "lowercase"
      }
    }
  },
  "contact": {
    "type": "object",
    "properties": {
      "name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      },
      "email": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      },
      "twitter": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      },
      "phone": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      }
    }
  },
  "partners": {
    "type": "nested",
    "properties": {
      "name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "email": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      }
    }
  },
  "permissions": {
    "type": "object",
    "properties": {
      "licenses": {
        "type": "nested",
        "properties": {
          "name": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "normalizer": "lowercase"
              }
            }
          },
          "URL": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      },
      "usageType": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      },
      "exemptionText": {
        "type": "text"
      }
    }
  },
  "laborHours": {
    "type": "integer"
  },
  "relatedCode": {
    "type": "nested",
    "properties": {
      "name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      },
      "URL": {
        "type": "keyword",
        "normalizer": "lowercase"
      }
    }
  },
  "reusedCode": {
    "type": "nested",
    "properties": {
      "name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "lowercase"
          }
        }
      },
      "URL": {
        "type": "keyword",
        "normalizer": "lowercase"
      }
    }
  },
  "disclaimerURL": {
    "type": "keyword",
    "normalizer": "lowercase"
  },
  "disclaimerText": {
    "type": "text"
  },
  "additionalInformation": {
    "type": "object",
    "dynamic": true
  },
  "date": {
    "type": "object",
    "properties": {
      "created": {
        "type": "date",
        "ignore_malformed": true
      },
      "lastModified": {
        "type": "date",
        "ignore_malformed": true
      },
      "metadataLastUpdated": {
        "type": "date",
        "ignore_malformed": true
      }
    }
  }
}
"""


"""
    find_all_repos(GSA_KEY::AbstractString = ENV["GSA_KEY"])

Return a list of all repositories in the Code.gov API.
"""
function find_all_repos(GSA_KEY::AbstractString = ENV["GSA_KEY"])
    response = request("GET",
                       "https://api.code.gov/repos?size=1000",
                       ["Content-Type" => "application/json",
                        "x-api-key" => GSA_KEY])
    json = JSON3.read(response.body)
    total = json.total
    output = DataFrame()
    DataFrame((pn = getproperty(json.repos[1], pn) for pn in propertynames(json.repos[1]))
    for row in json.repos
        push!(output, DataFrame(row), cols = :union)
    end

    DataFrame()
    chk = DataFrame(jsontable(json.repos))
    repos = DataFrame(parse_repository(repo) for repo in json.repos)
    allowmissing!(repos)
    eltype(repos.os)
    # unique!(repos)
    while size(repos, 1) < total
        println(size(repos, 1))
        response = request("GET",
                           "https://api.code.gov/repos?size=1000&from=$(size(repos, 1))",
                           ["Content-Type" => "application/json",
                            "x-api-key" => GSA_KEY])
        json = JSON3.read(response.body)
        append!(repos, DataFrame(parse_repository(repo) for repo in json.repos), promote = true)
    end
    @assert (length(repos) == total) && total < 10_000
    agencies
end


sql = """
-- Table: codegov.repos

-- DROP TABLE codegov.repos;

CREATE TABLE codegov.repos
(
    repoid text COLLATE pg_catalog."default" NOT NULL,
    score real,
    agency text COLLATE pg_catalog."default",
    status text COLLATE pg_catalog."default",
    vcs text COLLATE pg_catalog."default",
    repository_url text COLLATE pg_catalog."default",
    os text COLLATE pg_catalog."default",
    name text COLLATE pg_catalog."default",
    version text COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    tags text[] COLLATE pg_catalog."default",
    license text COLLATE pg_catalog."default",
    usage text COLLATE pg_catalog."default",
    organization text COLLATE pg_catalog."default",
    languages text[] COLLATE pg_catalog."default",
    laborhours real,
    homepage_url text COLLATE pg_catalog."default",
    download_url text COLLATE pg_catalog."default",
    created timestamp without time zone,
    last_modified timestamp without time zone,
    metadata_lastupdated timestamp without time zone,
    contact_name text COLLATE pg_catalog."default",
    contact_url text COLLATE pg_catalog."default",
    contact_email text COLLATE pg_catalog."default",
    contact_twitter text COLLATE pg_catalog."default",
    CONSTRAINT repos_pkey PRIMARY KEY (repoid)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE codegov.repos
    OWNER to jbs3hp;
"""


another = """
WITH A AS (
	SELECT DISTINCT agency, organization AS org, SUBSTRING(repository_url, '(?<=github.com/).*(?=/)') AS gh_org,
		   SUBSTRING(contact_url, '(?<=\@).*') as domain
	FROM codegov.repos
),
B AS (
	SELECT *
	FROM A
	WHERE gh_org IS NOT NULL
),
C AS (
	SELECT DISTINCT name, acronym, website
	FROM codegov.agencies
),
D AS (
	SELECT *
	FROM B
	LEFT JOIN C
	ON B.agency = C.acronym
)
SELECT name AS agency, acronym, org, gh_org, domain, website
FROM D
ORDER BY name ASC, org ASC
;
"""
end
