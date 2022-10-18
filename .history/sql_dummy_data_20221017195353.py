

from asyncio.windows_events import NULL


CREATE TABLE conditions(
    id int
    mrn int NOT NULL
    cond_id int NOT NULL
    PRIMARY KEY(id)
    FOREIGN KEY(cond_id) REFERENCES conditions(id)
)
