import { query, sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { queryKaleidos } from './lib/kaleidos';

/**
 * Convert results of select query to an array of objects.
 * @method parseResult
 * @return {Array}
 */
function parseResult(result) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => obj[key] = row[key].value);
    return obj;
  });
};

async function getMeetingUriFromKaleidos(kaleidosGraph, uuid) {
  return await queryKaleidos(`
    SELECT ?s
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ?s a <http://data.vlaanderen.be/ns/besluit#Zitting> ;
          <http://mu.semte.ch/vocabularies/core/uuid> ${sparqlEscapeString(uuid)} .
      }
    }
  `);
}

function constructMeetingInfo(kaleidosGraph, uuid) {
  return `
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    CONSTRUCT {
      ?s a besluit:Zitting ;
        mu:uuid ${sparqlEscapeString(uuid)} ;
        besluit:geplandeStart ?geplandeStart .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ?s a besluit:Zitting ;
          mu:uuid ${sparqlEscapeString(uuid)} ;
          besluit:geplandeStart ?geplandeStart .
      }
    }
  `;
}

function constructProcedurestapInfo(kaleidosGraph, meetingUri) {
  return `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

  CONSTRUCT {
    ?s a dbpedia:UnitOfWork ;
      mu:uuid ?uuid ;
      besluitvorming:heeftBevoegde ?heeftBevoegde .
  }
  WHERE {
    GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
      ${sparqlEscapeUri(meetingUri)} besluitvorming:behandelt ?agenda .
      ?agenda dct:hasPart ?agendapunt .
      ?s a dbpedia:UnitOfWork ;
        besluitvorming:isGeagendeerdVia ?agendapunt ;
        mu:uuid ?uuid ;
        besluitvorming:heeftBevoegde ?heeftBevoegde .
    }
  }`;
}

async function getProcedurestappenInfoFromTmp(tmpGraph) {
  return await query(`
    PREFIX dbpedia: <http://dbpedia.org/ontology/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

    SELECT ?s ?heeftBevoegde
    WHERE {
      GRAPH ${sparqlEscapeUri(tmpGraph)} {
        ?s a dbpedia:UnitOfWork ;
          besluitvorming:heeftBevoegde ?heeftBevoegde .
      }
    }
  `);
}

function constructNieuwsbriefInfo(kaleidosGraph, procedurestapInfo) {
  return `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    CONSTRUCT {
      ?s a besluitvorming:NieuwsbriefInfo ;
        mu:uuid ?uuid ;
        dct:title ?title ;
        ext:htmlInhoud ?htmlInhoud ;
        ext:themesOfSubcase ?themesOfSubcase .
      ${sparqlEscapeUri(procedurestapInfo.s)} besluitvorming:heeftBevoegde ?heeftBevoegde ;
        prov:generated ?s ;
        besluitvorming:isGeagendeerdVia ?agendapunt .
      ?agendapunt ext:prioriteit ?priorty .
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(kaleidosGraph)} {
        ?s a besluitvorming:NieuwsbriefInfo ;
          ext:afgewerkt \"true\"^^<http://mu.semte.ch/vocabularies/typed-literals/boolean> ;
          ^prov:generated ${sparqlEscapeUri(procedurestapInfo.s)} ;
          mu:uuid ?uuid ;
          dct:title ?title ;
          ext:htmlInhoud ?htmlInhoud .
        OPTIONAL { ?s ext:themesOfSubcase ?themesOfSubcase .}
        ${sparqlEscapeUri(procedurestapInfo.s)} besluitvorming:heeftBevoegde ?heeftBevoegde ;
          besluitvorming:isGeagendeerdVia ?agendapunt .
        ?agendapunt ext:prioriteit ?priorty .
      }
    }
  `;
}

async function constructLinkZittingNieuws(exportGraph, meetingUri) {
  return await query(`
    PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

    INSERT {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ${sparqlEscapeUri(meetingUri)} <http://mu.semte.ch/vocabularies/ext/publishedNieuwsbriefInfo> ?s .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(exportGraph)} {
        ?s a besluitvorming:NieuwsbriefInfo .
      }
    }
  `);
}

export {
  parseResult,
  getMeetingUriFromKaleidos,
  constructMeetingInfo,
  constructProcedurestapInfo,
  getProcedurestappenInfoFromTmp,
  constructNieuwsbriefInfo,
  constructLinkZittingNieuws
}
