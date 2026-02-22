declare module 'axios' {
  export interface AxiosResponse<T = any> {
    data: T;
    status: number;
    statusText: string;
    headers: Record<string, string>;
    config: unknown;
  }

  export interface AxiosStatic {
    post<T = any>(url: string, data?: any, config?: any): Promise<AxiosResponse<T>>;
  }

  const axios: AxiosStatic;
  export default axios;
}

declare module 'ws' {
  export class WebSocket {
    on(event: string, listener: (...args: any[]) => void): this;
    send(data: string | Buffer): void;
    close(code?: number, data?: string): void;
  }

  export class WebSocketServer {
    constructor(options?: any);
    on(event: string, listener: (...args: any[]) => void): this;
  }
}

declare module 'chokidar' {
  export interface FSWatcher {
    on(event: string, listener: (...args: any[]) => void): this;
    close(): Promise<void>;
  }

  export interface WatchOptions {
    ignoreInitial?: boolean;
    awaitWriteFinish?: {
      stabilityThreshold?: number;
      pollInterval?: number;
    };
    ignored?: RegExp | string | ((path: string) => boolean);
  }

  export interface ChokidarStatic {
    watch(paths: string | string[], options?: WatchOptions): FSWatcher;
  }

  const chokidar: ChokidarStatic;
  export default chokidar;
}
