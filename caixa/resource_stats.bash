#!/bin/bash
# Fica em tail eterno nos arquivos com sufixo no nome do script, dentro do diretorio de
# resource stats no nfs. O nome do script eh usado porque nao consigo fazer o flock
# funcionar com parâmetro.
# pega informacoes adicionais do arquivo excludes.conf

set -m;
PATH=$PATH:/opt/postgres/current/bin;
csv_orig=/apl/nfs/broker/resourcestats;
dir_base=$(dirname $(dirname $(readlink -e $0)));

arquivo=${1,,}; #deixa lowercase pra normalizar pro flock
[[ -z $arquivo ]] && echo Informe sufixo do arquivo a monitorar. && exit 1;
[ "${FLOCKER}" != "$0" ] && exec env FLOCKER="$0" flock -en "$dir_base/lockfiles/$arquivo" "$0" "$@" || :;

read arquivo exclude <<< $(grep -i "^$arquivo" ${dir_base}/script/excludes.conf);
if [[ -z $arquivo || -z $exclude ]]; then
  echo "$(date '+%F %T') ERRO O arquivo ${dir_base}/script/excludes.conf nao contem chave para arquivo ${1}. Atualizar." >> ${dir_base}/log/$(date '+%F').watch.log;
  exit 1;
fi
proc=WATCH-RS-${arquivo^^};

find ${dir_base}/tmp/ -user $USER -name "*${arquivo,,}" | xargs chown -f storm. 2>/dev/null ;
find ${dir_base}/log/ -user $USER                       | xargs chown -f storm. 2>/dev/null ;

#tratamento pre-morte
fim () {
 find ${dir_base}/tmp/ -user $USER -name "*${tipo}" | xargs chown -f storm. 2>/dev/null ;
 echo "$(date '+%F %T') INFO FINALIZANDO POR ${1}. MATANDO PROCESSOS FILHOS" >> ${dir_base}/log/$(date '+%F').watch.log;
 echo "$(date '+%F %T') INFO FIM" >> ${dir_base}/log/$(date '+%F').watch.log;
 exit 0;
}
trap "fim SIGTERM" SIGTERM;
trap "fim SIGINT"  SIGINT ;

#Pega a quantidade de colunas do arquivo usando o cabecalho. Necessario porque
# em alguns CSVs o broker cospe mais coluna no corpo do arquivo do que no cabecalho =/
maxcols=$(head -qn1 ${csv_orig}/*${arquivo}.txt | head -1 | awk -F, '{print NF}');

exec -a "$proc awk" awk -F',' -v exclude="$exclude" -v d=${dir_base}/tmp -v l=${dir_base}/log -v fn="${arquivo,,}" -v mc=$maxcols '
  BEGIN {
    ts = strftime("%Y%m%d_%H%M");
    of = d"/"ts"_"fn;
    lf = l"/"strftime("%Y-%m-%d")".watch.log";
    system ("chown storm. "lf);
    print strftime("%Y-%m-%d %H:%M:%S")" "fn" INFO INICIO "of >> lf;
  }
  !/BrokerName/{
    if (ts != strftime("%Y%m%d_%H%M")) {
      system ("chown storm. "of);
      close(of);
      ts = strftime("%Y%m%d_%H%M");
      of = d"/"ts"_"fn;
      lf = l"/"strftime("%Y-%m-%d")".watch.log";
      system ("chown storm. "lf);
      print strftime("%Y-%m-%d %H:%M:%S")" "fn" INFO ROTATE "of >> lf;
      fflush(lf);
    }

    o="";
    for (i=1; i<=mc; i++) {
      if (i !~ exclude) {
        o=o""$i","
      }
    }
    print substr(o, 1, length(o)-1) >> of;
  }
' < <(tail -Fq ${csv_orig}/*${arquivo}.txt | tr -dc '[[:print:]]\n');