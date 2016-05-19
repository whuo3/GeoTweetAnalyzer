import {Component} from 'angular2/core';

@Component({
  selector: 'global-rank',
  template: `
    <ol>
      <li *ngFor='let rank of globalRank'>
        <a href='https://twitter.com/hashtag/{{rank}}' target='_blank'>{{rank}}</a>
      </li>
    </ol>
  `
})

export class GlobalRankComponent {
  globalRank = [];
  socket = null;

  constructor() {
    this.socket = io();
    this.socket.on('world.ranking', function(rank:any) {
      this.globalRank = rank;
    }.bind(this));
  }
}
